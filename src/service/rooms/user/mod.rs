use std::{collections::BTreeMap, sync::Arc};

use conduwuit::{Result, implement, warn};
use database::{Database, Deserialized, Map};
use futures::StreamExt;
use ruma::{EventId, OwnedEventId, OwnedRoomId, OwnedUserId, RoomId, UserId};

use crate::{Dep, globals, rooms, rooms::short::ShortStateHash};

pub struct Service {
	db: Data,
	services: Services,
}

struct Data {
	db: Arc<Database>,
	userroomid_notificationcount: Arc<Map>,
	userroomid_highlightcount: Arc<Map>,
	userroomidthreadid_notificationcount: Arc<Map>,
	userroomidthreadid_highlightcount: Arc<Map>,
	roomuserid_lastnotificationread: Arc<Map>,
	roomsynctoken_shortstatehash: Arc<Map>,
}

struct Services {
	globals: Dep<globals::Service>,
	short: Dep<rooms::short::Service>,
}

impl crate::Service for Service {
	fn build(args: crate::Args<'_>) -> Result<Arc<Self>> {
		Ok(Arc::new(Self {
			db: Data {
				db: args.db.clone(),
				userroomid_notificationcount: args.db["userroomid_notificationcount"].clone(),
				userroomid_highlightcount: args.db["userroomid_highlightcount"].clone(),
				userroomidthreadid_notificationcount: args.db
					["userroomidthreadid_notificationcount"]
					.clone(),
				userroomidthreadid_highlightcount: args.db["userroomidthreadid_highlightcount"]
					.clone(),
				roomuserid_lastnotificationread: args.db["userroomid_highlightcount"].clone(),
				roomsynctoken_shortstatehash: args.db["roomsynctoken_shortstatehash"].clone(),
			},

			services: Services {
				globals: args.depend::<globals::Service>("globals"),
				short: args.depend::<rooms::short::Service>("rooms::short"),
			},
		}))
	}

	fn name(&self) -> &str { crate::service::make_name(std::module_path!()) }
}

#[implement(Service)]
pub fn reset_notification_counts(&self, user_id: &UserId, room_id: &RoomId) {
	let userroom_id = (user_id, room_id);
	self.db.userroomid_highlightcount.put(userroom_id, 0_u64);
	self.db.userroomid_notificationcount.put(userroom_id, 0_u64);

	let roomuser_id = (room_id, user_id);
	let count = self.services.globals.next_count().unwrap();
	self.db
		.roomuserid_lastnotificationread
		.put(roomuser_id, count);
}

#[implement(Service)]
pub fn stream_notification_counts<'a>(
	&'a self,
	user_id: &'a UserId,
) -> impl futures::Stream<Item = (Result<OwnedRoomId>, u64)> + Send + 'a {
	let prefix = (user_id, database::Interfix);
	self.db
		.userroomid_notificationcount
		.stream_prefix::<(OwnedUserId, OwnedRoomId), u64, _>(&prefix)
		.map(|res| match res {
			| Ok(((_user_id, room_id), count)) => (Ok(room_id), count),
			| Err(e) => {
				warn!("Failed to stream notification counts: {e}");
				(Err(e), 0)
			},
		})
}

#[implement(Service)]
pub async fn notification_count(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (user_id, room_id);
	self.db
		.userroomid_notificationcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

#[implement(Service)]
pub async fn highlight_count(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (user_id, room_id);
	self.db
		.userroomid_highlightcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

#[implement(Service)]
pub fn reset_thread_notification_counts(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	thread_id: &EventId,
) {
	let key = (user_id, room_id, thread_id);
	self.db.userroomidthreadid_notificationcount.put(key, 0_u64);
	self.db.userroomidthreadid_highlightcount.put(key, 0_u64);

	let roomuser_id = (room_id, user_id);
	let count = self.services.globals.next_count().unwrap();
	self.db
		.roomuserid_lastnotificationread
		.put(roomuser_id, count);
}

#[implement(Service)]
pub async fn thread_notification_count(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	thread_id: &EventId,
) -> u64 {
	let key = (user_id, room_id, thread_id);
	self.db
		.userroomidthreadid_notificationcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

#[implement(Service)]
pub async fn thread_highlight_count(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
	thread_id: &EventId,
) -> u64 {
	let key = (user_id, room_id, thread_id);
	self.db
		.userroomidthreadid_highlightcount
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

/// Get all per-thread unread notification counts for a user in a room.
#[implement(Service)]
pub async fn thread_notification_counts(
	&self,
	user_id: &UserId,
	room_id: &RoomId,
) -> BTreeMap<OwnedEventId, (u64, u64)> {
	let prefix = (user_id, room_id, database::Interfix);
	let mut result = BTreeMap::new();

	let mut stream = self
		.db
		.userroomidthreadid_notificationcount
		.stream_prefix::<(OwnedUserId, OwnedRoomId, OwnedEventId), u64, _>(&prefix);

	while let Some(item) = stream.next().await {
		if let Ok(((_uid, _rid, thread_id), count)) = item {
			if count > 0 {
				result.entry(thread_id).or_insert((0, 0)).0 = count;
			}
		}
	}

	let mut highlight_stream =
		self.db
			.userroomidthreadid_highlightcount
			.stream_prefix::<(OwnedUserId, OwnedRoomId, OwnedEventId), u64, _>(&prefix);

	while let Some(item) = highlight_stream.next().await {
		if let Ok(((_uid, _rid, thread_id), count)) = item {
			if count > 0 {
				result.entry(thread_id).or_insert((0, 0)).1 = count;
			}
		}
	}

	// Remove entries where both counts are zero
	result.retain(|_, (n, h)| *n > 0 || *h > 0);
	result
}

#[implement(Service)]
pub async fn last_notification_read(&self, user_id: &UserId, room_id: &RoomId) -> u64 {
	let key = (room_id, user_id);
	self.db
		.roomuserid_lastnotificationread
		.qry(&key)
		.await
		.deserialized()
		.unwrap_or(0)
}

#[implement(Service)]
pub async fn associate_token_shortstatehash(
	&self,
	room_id: &RoomId,
	token: u64,
	shortstatehash: ShortStateHash,
) {
	let shortroomid = self
		.services
		.short
		.get_shortroomid(room_id)
		.await
		.expect("room exists");

	let _cork = self.db.db.cork();
	let key: &[u64] = &[shortroomid, token];
	self.db
		.roomsynctoken_shortstatehash
		.put(key, shortstatehash);
}

#[implement(Service)]
pub async fn get_token_shortstatehash(
	&self,
	room_id: &RoomId,
	token: u64,
) -> Result<ShortStateHash> {
	let shortroomid = self.services.short.get_shortroomid(room_id).await?;

	let key: &[u64] = &[shortroomid, token];
	self.db
		.roomsynctoken_shortstatehash
		.qry(key)
		.await
		.deserialized()
}
