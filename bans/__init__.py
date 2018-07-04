import uuid
import time
import re
from ipaddress import ip_address
from datetime import datetime, timezone

from sqlalchemy import Table, Column, PrimaryKeyConstraint, Binary, Boolean, Integer, String, MetaData, ForeignKey
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import select, func, and_, or_
from sqlalchemy.sql.expression import cast

from sqlalchemy_uuid import UUID

class Ban:
	def __init__(
			self,
			ban_uuid,
			remote_origin,
			scope='global',
			user_uuid=uuid.UUID('00000000-0000-0000-0000-000000000000'),
			creation_time=0,
			expiration_time=0,
			view_time=0,
			created_by_uuid=uuid.UUID('00000000-0000-0000-0000-000000000000'),
			reason='',
			note='',
		):

		self.uuid = ban_uuid
		self.remote_origin = ip_address(remote_origin)

		if not scope:
			scope = 'global'
		self.scope = scope

		if isinstance(user_uuid, str):
			user_uuid = uuid.UUID(user_uuid)
		elif isinstance(user_uuid, bytes):
			user_uuid = uuid.UUID(bytes=user_uuid)
		elif not isinstance(user_uuid, uuid.UUID):
			raise TypeError('User uuid must be UUID (received ' + str(type(user_uuid)) + ')')
		self.user_uuid = user_uuid

		self.creation_time = datetime.fromtimestamp(creation_time, timezone.utc)
		self.expiration_time = datetime.fromtimestamp(expiration_time, timezone.utc)
		self.view_time = datetime.fromtimestamp(view_time, timezone.utc)

		if isinstance(created_by_uuid, str):
			created_by_uuid = uuid.UUID(created_by_uuid)
		elif isinstance(created_by_uuid, bytes):
			created_by_uuid = uuid.UUID(bytes=created_by_uuid)
		elif not isinstance(created_by_uuid, uuid.UUID):
			raise TypeError('Created by uuid must be UUID (received ' + str(type(created_by_uuid)) + ')')
		self.created_by_uuid = created_by_uuid

		self.reason = reason
		self.note = note

def list_to_item_uuid_dictionary(list):
	dict = {}
	for item in list:
		dict[item.uuid] = item
	return dict

def list_to_users_item_uuid_dictionary(list, attr):
	dict = {}
	for item in list:
		if not item.user.uuid in dict:
			dict[item.user.uuid] = {}
		dict[item.user.uuid][getattr(item, attr)] = item
	return dict

class Bans:
	def __init__(self, engine):
		self.engine = engine
		self.engine_session = sessionmaker(bind=self.engine)()

		self.scope_length = 16
		self.reason_length = 64
		self.note_length = 64

		metadata = MetaData()

		default_uuid = uuid.UUID('00000000-0000-0000-0000-000000000000')
		default_origin = ip_address(0b0 * 16)

		# bans tables
		self.bans = Table('bans', metadata,
			Column('uuid', UUID, primary_key=True, default=default_uuid),
			Column('scope', String(self.scope_length), default=''),
			Column('remote_origin', Binary(16), default=default_origin),
			Column('user_uuid', UUID, default=default_uuid),
			Column('creation_time', Integer, default=0),
			Column('expiration_time', Integer, default=0),
			Column('view_time', Integer, default=0),
			Column('created_by_uuid', UUID, default=default_uuid),
			Column('reason', String(self.reason_length), default=''),
			Column('note', String(self.note_length), default=''),
		)

		table_exists = self.engine.dialect.has_table(self.engine, 'bans')

		self.connection = self.engine.connect()

		if not table_exists:
			metadata.create_all(self.engine)

	# bans
	def create_ban(self, remote_origin, ban_uuid=None, **kwargs):
		if not ban_uuid:
			ban_uuid = uuid.uuid4()
		if 'creation_time' not in kwargs:
			kwargs['creation_time'] = time.time()

		ban = Ban(ban_uuid, remote_origin, **kwargs)

		self.connection.execute(
			self.bans.insert(),
			uuid=ban.uuid,
			remote_origin=ban.remote_origin.packed,
			user_uuid=ban.user_uuid,
			creation_time=ban.creation_time.timestamp(),
			expiration_time=ban.expiration_time.timestamp(),
			created_by_uuid=ban.created_by_uuid,
			reason=ban.reason,
			note=ban.note,
		)
		return ban

	def remove_ban(self, ban):
		self.connection.execute(
			self.bans.delete().where(self.bans.c.uuid == ban.uuid)
		)

	def update_ban(self, ban, **kwargs):
		updates = {}
		if 'scope' in kwargs:
			updates['scope'] = str(kwargs['scope'])
		if 'remote_origin' in kwargs:
			#TODO type checking here
			updates['remote_origin'] = ip_address(str(kwargs['remote_origin'])).packed
		if 'user_uuid' in kwargs:
			#TODO typechecking here
			updates['user_uuid'] = kwargs['user_uuid']
		if 'expiration_time' in kwargs:
			updates['expiration_time'] = int(kwargs['expiration_time'])
		if 'view_time' in kwargs:
			updates['view_time'] = int(kwargs['view_time'])
		if 'reason' in kwargs:
			updates['reason'] = str(kwargs['reason'])
		if 'note' in kwargs:
			updates['note'] = str(kwargs['note'])
		if 0 == len(updates):
			return
		self.connection.execute(
			self.bans.update().values(**updates).where(self.bans.c.uuid == ban.uuid)
		)

	def get_ban(self, ban_uuid):
		bans = self.search_bans(filter={'uuids': ban_uuid})
		if 1 != len(bans) or getattr(bans[0], 'uuid') != ban_uuid:
			return None
		return bans[0]

	def prepare_bans_search_conditions(self, filter):
		conditions = []
		if 'uuids' in filter:
			if list is not type(filter['uuids']):
				filter['uuids'] = [filter['uuids']]
			block_conditions = []
			for ban_uuid in filter['uuids']:
				block_conditions.append(self.bans.c.uuid == ban_uuid)
			conditions.append(or_(*block_conditions))
		if 'remote_origins' in filter:
			if list is not type(filter['remote_origins']):
				filter['remote_origins'] = [filter['remote_origins']]
			block_conditions = []
			for remote_origin in filter['remote_origins']:
				try:
					remote_origin = ip_address(str(remote_origin))
				except ValueError:
					conditions.append(False)
					break
				else:
					block_conditions.append(or_(self.bans.c.remote_origin == remote_origin.packed))
			conditions.append(and_(*block_conditions))
		if 'scopes' in filter:
			if list is not type(filter['scopes']):
				filter['scopes'] = [filter['scopes']]
			block_conditions = []
			for scope in filter['scopes']:
				block_conditions.append(or_(self.bans.c.scope == str(scope)))
			conditions.append(and_(*block_conditions))
		if 'user_uuids' in filter:
			if list is not type(filter['user_uuids']):
				filter['user_uuids'] = [filter['user_uuids']]
			block_conditions = []
			for user_uuid in filter['user_uuids']:
				try:
					block_conditions.append(or_(self.bans.c.user_uuid == user_uuid))
				except ValueError:
					continue
			conditions.append(and_(*block_conditions))
		if 'created_after' in filter:
			conditions.append(self.bans.c.creation_time > int(filter['created_after']))
		if 'created_before' in filter:
			conditions.append(self.bans.c.creation_time <= int(filter['created_before']))
		if 'expires_after' in filter:
			conditions.append(self.bans.c.expiration_time > int(filter['expires_after']))
		if 'expires_before' in filter:
			conditions.append(self.bans.c.expiration_time != 0)
			conditions.append(self.bans.c.expiration_time <= int(filter['expires_before']))
		if 'viewed_after' in filter:
			conditions.append(self.bans.c.view_time > int(filter['viewed_after']))
		if 'viewed_before' in filter:
			conditions.append(self.bans.c.view_time != 0)
			conditions.append(self.bans.c.view_time <= int(filter['viewed_before']))
		if 'created_by_uuids' in filter:
			if list is not type(filter['created_by_uuids']):
				filter['created_by_uuids'] = [filter['created_by_uuids']]
			block_conditions = []
			for created_by_uuid in filter['created_by_uuids']:
				try:
					block_conditions.append(or_(self.bans.c.created_by_uuid == created_by_uuid))
				except ValueError:
					continue
			conditions.append(and_(*block_conditions))
		if 'reasons' in filter:
			if list is not type(filter['reasons']):
				filter['reasons'] = [filter['reasons']]
			block_conditions = []
			for reason in filter['reasons']:
				block_conditions.append(or_(self.bans.c.reason.like(reason, escape='\\')))
			conditions.append(and_(*block_conditions))
		if 'notes' in filter:
			if list is not type(filter['notes']):
				filter['notes'] = [filter['notes']]
			block_conditions = []
			for note in filter['notes']:
				block_conditions.append(or_(self.bans.c.note.like(note, escape='\\')))
			conditions.append(and_(*block_conditions))
		return conditions

	def count_bans(self, filter={}):
		conditions = self.prepare_bans_search_conditions(filter)
		if 0 == len(conditions):
			statement = self.bans.select().count()
		else:
			statement = self.bans.select().where(and_(*conditions)).count()

		return self.connection.execute(statement).fetchall()[0][0]

	def search_bans(self, filter={}, sort='creation_time', order='desc', page=0, perpage=None):
		conditions = self.prepare_bans_search_conditions(filter)
		if 0 == len(conditions):
			statement = self.bans.select()
		else:
			statement = self.bans.select().where(and_(*conditions))

		try:
			sort_column = getattr(self.bans.c, sort)
		except:
			sort_column = self.bans.c.creation_time

		if 'desc' != order:
			order = 'asc'

		column_order = getattr(sort_column, order)
		statement = statement.order_by(column_order())
		# always secondary sort by uuid
		statement = statement.order_by(getattr(self.bans.c.uuid, order)())

		if 0 < page:
			statement = statement.offset(page * perpage)
		if perpage:
			statement = statement.limit(perpage)

		result = self.connection.execute(statement).fetchall()
		if 0 == len(result):
			return []

		bans = []
		for row in result:
			ban = Ban(
				row[self.bans.c.uuid],
				row[self.bans.c.remote_origin],
				scope=row[self.bans.c.scope],
				user_uuid=row[self.bans.c.user_uuid],
				creation_time=row[self.bans.c.creation_time],
				expiration_time=row[self.bans.c.expiration_time],
				view_time=row[self.bans.c.view_time],
				created_by_uuid=row[self.bans.c.created_by_uuid],
				reason=row[self.bans.c.reason],
				note=row[self.bans.c.note]
			)

			bans.append(ban)
		return bans

	def bans_dictionary(self, list):
		return list_to_item_uuid_dictionary(list)

	def prune_bans(self, cutoff_time):
		self.connection.execute(
			self.bans.delete().where(
				and_(
					self.bans.c.expiration_time < cutoff_time,
					self.bans.c.expiration_time > 0
					#TODO don't prune unviewed bans?
				)
			)
		)

	def check_ban(self, scope='', remote_origin='', user_uuid=None):
		expiration_conditions = [
			self.bans.c.expiration_time == 0,
			self.bans.c.expiration_time > time.time()
		]
		scope_conditions = [
			self.bans.c.scope == 'global',
		]
		if scope and 'global' != scope:
			scope_conditions.append(self.bans.c.scope == scope)

		conditions = []
		if remote_origin:
			#TODO type checking
			conditions.append(self.bans.c.remote_origin == ip_address(str(remote_origin)).packed)

		if user_uuid:
			#TODO type checking
			conditions.append(self.bans.c.user_uuid == user_uuid)

		if not conditions:
			raise ValueError('Neither remote origin or user uuid provided')

		statement = self.bans.select().where(
			and_(
				or_(*expiration_conditions),
				or_(*scope_conditions),
				or_(*conditions)
			)
		).order_by(self.bans.c.expiration_time.desc()).limit(1)
		row = self.connection.execute(statement).fetchone()
		if not row:
			return None

		ban = Ban(
			row[self.bans.c.uuid],
			row[self.bans.c.remote_origin],
			scope=row[self.bans.c.scope],
			user_uuid=row[self.bans.c.user_uuid],
			creation_time=row[self.bans.c.creation_time],
			expiration_time=row[self.bans.c.expiration_time],
			view_time=row[self.bans.c.view_time],
			created_by_uuid=row[self.bans.c.created_by_uuid],
			reason=row[self.bans.c.reason],
			note=row[self.bans.c.note]
		)

		return ban
