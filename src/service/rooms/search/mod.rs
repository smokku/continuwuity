use std::{collections::BTreeSet, sync::Arc};

use conduwuit::{
	PduCount, PduEvent, Result,
	arrayvec::ArrayVec,
	debug_warn, implement,
	matrix::event::{Event, Matches},
	utils::{
		ArrayVecExt, IterStream, ReadyExt, set,
		stream::{TryIgnore, WidebandExt},
	},
};
use database::{Map, keyval::Val};
use futures::{Stream, StreamExt};
use ruma::{OwnedUserId, RoomId, UserId, api::client::search::search_events::v3::Criteria};
use serde::Deserialize;

use crate::{
	Dep, rooms,
	rooms::{
		short::ShortRoomId,
		timeline::{PduId, RawPduId},
	},
};

pub struct Service {
	db: Data,
	services: Services,
}

struct Data {
	tokenids: Arc<Map>,
	mentionuserids: Arc<Map>,
	mentionroomids: Arc<Map>,
}

struct Services {
	short: Dep<rooms::short::Service>,
	state_accessor: Dep<rooms::state_accessor::Service>,
	timeline: Dep<rooms::timeline::Service>,
	pdu_metadata: Dep<rooms::pdu_metadata::Service>,
}

#[derive(Clone, Debug)]
pub struct RoomQuery<'a> {
	pub room_id: &'a RoomId,
	pub user_id: Option<&'a UserId>,
	pub criteria: &'a Criteria,
	pub mentions: &'a MentionQuery,
	pub limit: usize,
	pub skip: usize,
}

#[derive(Clone, Debug, Default)]
pub struct MentionQuery {
	pub user_ids: Vec<OwnedUserId>,
	pub room: Option<bool>,
}

type TokenId = ArrayVec<u8, TOKEN_ID_MAX_LEN>;

const TOKEN_ID_MAX_LEN: usize =
	size_of::<ShortRoomId>() + WORD_MAX_LEN + 1 + size_of::<RawPduId>();
const WORD_MAX_LEN: usize = 50;

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				tokenids: args.db["tokenids"].clone(),
				mentionuserids: args.db["mentionuserids"].clone(),
				mentionroomids: args.db["mentionroomids"].clone(),
			},
			services: Services {
				short: args.depend::<rooms::short::Service>("rooms::short"),
				state_accessor: args
					.depend::<rooms::state_accessor::Service>("rooms::state_accessor"),
				timeline: args.depend::<rooms::timeline::Service>("rooms::timeline"),
				pdu_metadata: args.depend::<rooms::pdu_metadata::Service>("rooms::pdu_metadata"),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
pub fn index_pdu(&self, shortroomid: ShortRoomId, pdu_id: &RawPduId, message_body: &str) {
	let batch = tokenize(message_body)
		.map(|word| {
			let mut key = shortroomid.to_be_bytes().to_vec();
			key.extend_from_slice(word.as_bytes());
			key.push(0xFF);
			key.extend_from_slice(pdu_id.as_ref()); // TODO: currently we save the room id a second time here
			key
		})
		.collect::<Vec<_>>();

	self.db
		.tokenids
		.insert_batch(batch.iter().map(|k| (k.as_slice(), &[])));
}

#[implement(Service)]
pub fn deindex_pdu(&self, shortroomid: ShortRoomId, pdu_id: &RawPduId, message_body: &str) {
	let batch = tokenize(message_body).map(|word| {
		let mut key = shortroomid.to_be_bytes().to_vec();
		key.extend_from_slice(word.as_bytes());
		key.push(0xFF);
		key.extend_from_slice(pdu_id.as_ref()); // TODO: currently we save the room id a second time here
		key
	});

	for token in batch {
		self.db.tokenids.remove(&token);
	}
}

#[implement(Service)]
pub fn index_pdu_mentions<Pdu>(
	&self,
	shortroomid: ShortRoomId,
	pdu_id: &RawPduId,
	pdu: &Pdu,
) -> usize
where
	Pdu: Event,
{
	let mentions = extract_mentions(pdu);
	let count = usize::from(mentions.room).saturating_add(mentions.user_ids.len());

	if mentions.room {
		let room_mention = make_mention_roomid(shortroomid, pdu_id);
		self.db.mentionroomids.insert(&room_mention, []);
	}

	let user_mentions = mentions
		.user_ids
		.iter()
		.map(|user_id| make_mention_userid(shortroomid, user_id, pdu_id))
		.collect::<Vec<_>>();

	self.db
		.mentionuserids
		.insert_batch(user_mentions.iter().map(|k| (k.as_slice(), &[])));

	count
}

#[implement(Service)]
pub fn deindex_pdu_mentions<Pdu>(&self, shortroomid: ShortRoomId, pdu_id: &RawPduId, pdu: &Pdu)
where
	Pdu: Event,
{
	let mentions = extract_mentions(pdu);
	if mentions.room {
		let room_mention = make_mention_roomid(shortroomid, pdu_id);
		self.db.mentionroomids.remove(&room_mention);
	}

	for user_id in mentions.user_ids {
		let key = make_mention_userid(shortroomid, &user_id, pdu_id);
		self.db.mentionuserids.remove(&key);
	}
}

#[implement(Service)]
pub async fn search_pdus<'a>(
	&'a self,
	query: &'a RoomQuery<'a>,
	sender_user: &'a UserId,
) -> Result<(usize, impl Stream<Item = PduEvent> + Send + 'a)> {
	let pdu_ids: Vec<_> = self.search_pdu_ids(query).await?.collect().await;

	let filter = &query.criteria.filter;
	let count = pdu_ids.len();
	let pdus = pdu_ids
		.into_iter()
		.stream()
		.wide_filter_map(move |result_pdu_id: RawPduId| async move {
			self.services
				.timeline
				.get_pdu_from_id(&result_pdu_id)
				.await
				.ok()
		})
		.ready_filter(|pdu| !pdu.is_redacted())
		.ready_filter(move |pdu| filter.matches(pdu))
		.wide_filter_map(move |pdu| async move {
			self.services
				.state_accessor
				.user_can_see_event(query.user_id?, pdu.room_id().unwrap(), pdu.event_id())
				.await
				.then_some(pdu)
		})
		.skip(query.skip)
		.take(query.limit)
		.map(move |mut pdu| {
			pdu.set_unsigned(query.user_id);

			pdu
		})
		.then(async move |mut pdu| {
			if let Err(e) = self
				.services
				.pdu_metadata
				.add_bundled_aggregations_to_pdu(sender_user, &mut pdu)
				.await
			{
				debug_warn!("Failed to add bundled aggregations: {e}");
			}
			pdu
		});

	Ok((count, pdus))
}

// result is modeled as a stream such that callers don't have to be refactored
// though an additional async/wrap still exists for now
#[implement(Service)]
pub async fn search_pdu_ids<'a>(
	&'a self,
	query: &'a RoomQuery<'_>,
) -> Result<impl Stream<Item = RawPduId> + Send + 'a + use<'a>> {
	let shortroomid = self.services.short.get_shortroomid(query.room_id).await?;
	let mut indexed_pdu_ids = self.search_pdu_ids_query_room(query, shortroomid).await;

	if !query.mentions.user_ids.is_empty() {
		let pdu_ids = self
			.search_pdu_ids_query_mentions_userids(shortroomid, &query.mentions.user_ids)
			.await;
		indexed_pdu_ids.push(pdu_ids);
	}

	if query.mentions.room == Some(true) {
		let pdu_ids = self
			.search_pdu_ids_query_mentions_room(shortroomid)
			.collect::<Vec<_>>()
			.await;
		indexed_pdu_ids.push(pdu_ids);
	}

	if indexed_pdu_ids.is_empty() {
		let pdu_ids = self
			.search_pdu_ids_query_all(shortroomid, query.room_id)
			.collect::<Vec<_>>()
			.await;
		return Ok(pdu_ids.into_iter().stream());
	}

	let pdu_ids = set::intersection(indexed_pdu_ids.into_iter().map(IntoIterator::into_iter))
		.collect::<Vec<_>>();

	Ok(pdu_ids.into_iter().stream())
}

#[implement(Service)]
async fn search_pdu_ids_query_room(
	&self,
	query: &RoomQuery<'_>,
	shortroomid: ShortRoomId,
) -> Vec<Vec<RawPduId>> {
	tokenize(&query.criteria.search_term)
		.stream()
		.wide_then(|word| async move {
			self.search_pdu_ids_query_words(shortroomid, &word)
				.collect::<Vec<_>>()
				.await
		})
		.collect::<Vec<_>>()
		.await
}

#[implement(Service)]
fn search_pdu_ids_query_all<'a>(
	&'a self,
	shortroomid: ShortRoomId,
	room_id: &'a RoomId,
) -> impl Stream<Item = RawPduId> + Send + 'a {
	self.services
		.timeline
		.pdus_rev(room_id, None)
		.ignore_err()
		.map(move |(shorteventid, _)| PduId { shortroomid, shorteventid }.into())
}

#[implement(Service)]
async fn search_pdu_ids_query_mentions_userids(
	&self,
	shortroomid: ShortRoomId,
	user_ids: &[OwnedUserId],
) -> Vec<RawPduId> {
	let mut pdu_ids = Vec::new();
	for user_id in user_ids {
		pdu_ids.extend(
			self.search_pdu_ids_query_mentions_userid(shortroomid, user_id)
				.collect::<Vec<_>>()
				.await,
		);
	}

	pdu_ids.sort_unstable_by(|a, b| b.as_ref().cmp(a.as_ref()));
	pdu_ids.dedup();
	pdu_ids
}

#[implement(Service)]
fn search_pdu_ids_query_mentions_userid<'a>(
	&'a self,
	shortroomid: ShortRoomId,
	user_id: &'a UserId,
) -> impl Stream<Item = RawPduId> + Send + 'a {
	self.search_pdu_ids_query_mentions_userid_raw(shortroomid, user_id)
		.map(move |key| -> RawPduId {
			let key = &key[mention_user_prefix_len(user_id)..];
			key.into()
		})
}

#[implement(Service)]
fn search_pdu_ids_query_mentions_userid_raw<'a>(
	&'a self,
	shortroomid: ShortRoomId,
	user_id: &'a UserId,
) -> impl Stream<Item = Val<'a>> + Send + 'a + use<'a> {
	let prefix = make_mention_user_prefix(shortroomid, user_id);
	self.db
		.mentionuserids
		.raw_keys_from(&prefix)
		.ignore_err()
		.ready_take_while(move |key| key.starts_with(&prefix))
}

#[implement(Service)]
fn search_pdu_ids_query_mentions_room(
	&self,
	shortroomid: ShortRoomId,
) -> impl Stream<Item = RawPduId> + Send + '_ {
	self.search_pdu_ids_query_mentions_room_raw(shortroomid)
		.map(move |key| -> RawPduId {
			let key = &key[MENTION_ROOM_PREFIX_LEN..];
			key.into()
		})
}

#[implement(Service)]
fn search_pdu_ids_query_mentions_room_raw(
	&self,
	shortroomid: ShortRoomId,
) -> impl Stream<Item = Val<'_>> + Send + '_ + use<'_> {
	let prefix = make_mention_room_prefix(shortroomid);
	self.db
		.mentionroomids
		.raw_keys_from(&prefix)
		.ignore_err()
		.ready_take_while(move |key| key.starts_with(&prefix))
}

/// Iterate over PduId's containing a word
#[implement(Service)]
fn search_pdu_ids_query_words<'a>(
	&'a self,
	shortroomid: ShortRoomId,
	word: &'a str,
) -> impl Stream<Item = RawPduId> + Send + 'a {
	self.search_pdu_ids_query_word(shortroomid, word)
		.map(move |key| -> RawPduId {
			let key = &key[prefix_len(word)..];
			key.into()
		})
}

/// Iterate over raw database results for a word
#[implement(Service)]
fn search_pdu_ids_query_word<'a>(
	&'a self,
	shortroomid: ShortRoomId,
	word: &'a str,
) -> impl Stream<Item = Val<'a>> + Send + 'a + use<'a> {
	// rustc says const'ing this not yet stable
	let end_id: RawPduId = PduId {
		shortroomid,
		shorteventid: PduCount::max(),
	}
	.into();

	// Newest pdus first
	let end = make_tokenid(shortroomid, word, &end_id);
	let prefix = make_prefix(shortroomid, word);
	self.db
		.tokenids
		.rev_raw_keys_from(&end)
		.ignore_err()
		.ready_take_while(move |key| key.starts_with(&prefix))
}

/// Splits a string into tokens used as keys in the search inverted index
///
/// This may be used to tokenize both message bodies (for indexing) or search
/// queries (for querying).
fn tokenize(body: &str) -> impl Iterator<Item = String> + Send + '_ {
	body.split_terminator(|c: char| !c.is_alphanumeric())
		.filter(|s| !s.is_empty())
		.filter(|word| word.len() <= WORD_MAX_LEN)
		.map(str::to_lowercase)
}

fn make_tokenid(shortroomid: ShortRoomId, word: &str, pdu_id: &RawPduId) -> TokenId {
	let mut key = make_prefix(shortroomid, word);
	key.extend_from_slice(pdu_id.as_ref());
	key
}

fn make_prefix(shortroomid: ShortRoomId, word: &str) -> TokenId {
	let mut key = TokenId::new();
	key.extend_from_slice(&shortroomid.to_be_bytes());
	key.extend_from_slice(word.as_bytes());
	key.push(database::SEP);
	key
}

fn prefix_len(word: &str) -> usize {
	size_of::<ShortRoomId>()
		.saturating_add(word.len())
		.saturating_add(1)
}

fn make_mention_userid(shortroomid: ShortRoomId, user_id: &UserId, pdu_id: &RawPduId) -> Vec<u8> {
	let mut key = make_mention_user_prefix(shortroomid, user_id);
	key.extend_from_slice(pdu_id.as_ref());
	key
}

fn make_mention_user_prefix(shortroomid: ShortRoomId, user_id: &UserId) -> Vec<u8> {
	let mut key = shortroomid.to_be_bytes().to_vec();
	key.extend_from_slice(user_id.as_str().as_bytes());
	key.push(database::SEP);
	key
}

fn mention_user_prefix_len(user_id: &UserId) -> usize {
	size_of::<ShortRoomId>()
		.saturating_add(user_id.as_str().len())
		.saturating_add(1)
}

fn make_mention_roomid(shortroomid: ShortRoomId, pdu_id: &RawPduId) -> Vec<u8> {
	let mut key = make_mention_room_prefix(shortroomid);
	key.extend_from_slice(pdu_id.as_ref());
	key
}

fn make_mention_room_prefix(shortroomid: ShortRoomId) -> Vec<u8> {
	let mut key = shortroomid.to_be_bytes().to_vec();
	key.push(database::SEP);
	key
}

const MENTION_ROOM_PREFIX_LEN: usize = size_of::<ShortRoomId>() + size_of::<u8>();

#[derive(Debug, Deserialize)]
struct ExtractMentions {
	#[serde(rename = "m.mentions")]
	mentions: Option<ExtractMentionsContent>,
}

#[derive(Debug, Deserialize)]
struct ExtractMentionsContent {
	#[serde(default)]
	user_ids: Vec<String>,
	room: Option<bool>,
}

#[derive(Debug, Default)]
struct IndexedMentions {
	user_ids: Vec<OwnedUserId>,
	room: bool,
}

fn extract_mentions<Pdu>(pdu: &Pdu) -> IndexedMentions
where
	Pdu: Event,
{
	let Ok(content) = pdu.get_content::<ExtractMentions>() else {
		return IndexedMentions::default();
	};

	let Some(mentions) = content.mentions else {
		return IndexedMentions::default();
	};

	let user_ids = mentions
		.user_ids
		.into_iter()
		.filter_map(|user_id| UserId::parse(&user_id).ok().map(ToOwned::to_owned))
		.collect::<BTreeSet<_>>()
		.into_iter()
		.collect::<Vec<_>>();

	IndexedMentions {
		user_ids,
		room: mentions.room.unwrap_or(false),
	}
}
