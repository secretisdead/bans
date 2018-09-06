import uuid
import time
import re
from ipaddress import ip_address
from datetime import datetime, timezone

from sqlalchemy import Table, Column, PrimaryKeyConstraint, LargeBinary
from sqlalchemy import Integer, String, MetaData, ForeignKey
from sqlalchemy.sql import func, and_, or_

from statement_helper import sort_statement, paginate_statement, id_filter
from statement_helper import time_filter, string_equal_filter
from statement_helper import string_like_filter, bitwise_filter
from base64_url import base64_url_encode, base64_url_decode
from idcollection import IDCollection
from parse_id import parse_id

def get_id_bytes(id):
	if isinstance(id, bytes):
		return id
	return base64_url_decode(id)

def generate_or_parse_id(id):
	if not id:
		id_bytes = uuid.uuid4().bytes
		id = base64_url_encode(id_bytes)
	else:
		id, id_bytes = parse_id(id)
	return (id, id_bytes)

class Ban:
	def __init__(
			self,
			id=None,
			creation_time=None,
			remote_origin='192.0.2.0',
			scope='',
			reason='',
			note='',
			expiration_time=0,
			view_time=0,
			created_by_user_id='',
			user_id='',
		):
		self.id, self.id_bytes = generate_or_parse_id(id)

		if None == creation_time:
			creation_time = time.time()
		self.creation_time = int(creation_time)
		self.creation_datetime = datetime.fromtimestamp(
			self.creation_time,
			timezone.utc,
		)

		self.remote_origin = ip_address(remote_origin)

		self.scope = scope

		self.reason = reason
		self.note = note

		self.expiration_time = int(expiration_time)
		self.expiration_datetime = datetime.fromtimestamp(
			self.expiration_time,
			timezone.utc,
		)

		self.view_time = int(view_time)
		self.view_datetime = datetime.fromtimestamp(
			self.view_time,
			timezone.utc,
		)

		self.created_by_user_id, self.created_by_user_id_bytes = parse_id(created_by_user_id)
		self.created_by_user = None

		self.user_id, self.user_id_bytes = parse_id(user_id)
		self.user = None

class Bans:
	def __init__(self, engine, db_prefix='', install=False):
		self.engine = engine

		self.db_prefix = db_prefix

		self.scope_length = 16
		self.reason_length = 64
		self.note_length = 64

		metadata = MetaData()

		default_bytes = 0b0 * 16
		default_origin = ip_address(0b0 * 16)

		# bans tables
		self.bans = Table(
			self.db_prefix + 'bans',
			metadata,
			Column(
				'id',
				LargeBinary(16),
				primary_key=True,
				default=default_bytes,
			),
			Column('creation_time', Integer, default=0),
			Column(
				'remote_origin',
				LargeBinary(16),
				default=ip_address(default_bytes).packed,
			),
			Column('scope', String(self.scope_length), default=''),
			Column('reason', String(self.reason_length), default=''),
			Column('note', String(self.note_length), default=''),
			Column('expiration_time', Integer, default=0),
			Column('view_time', Integer, default=0),
			Column('user_id', LargeBinary(16),default=default_bytes),
			Column(
				'created_by_user_id',
				LargeBinary(16),
				default=default_bytes,
			),
		)

		self.connection = self.engine.connect()

		if install:
			table_exists = self.engine.dialect.has_table(
				self.engine,
				self.db_prefix + 'bans'
			)
			if not table_exists:
				metadata.create_all(self.engine)

	def uninstall(self):
		self.bans.drop(self.engine)

	# retrieve bans
	def get_ban(self, id):
		bans = self.search_bans(filter={'ids': id})
		return bans.get(id)

	def prepare_bans_search_statement(self, filter):
		conditions = []
		conditions += id_filter(filter, 'ids', self.bans.c.id)
		conditions += time_filter(filter, 'created', self.bans.c.creation_time)
		if 'remote_origins' in filter:
			if list is not type(filter['remote_origins']):
				filter['remote_origins'] = [filter['remote_origins']]
			block_conditions = []
			for remote_origin in filter['remote_origins']:
				try:
					remote_origin = ip_address(str(remote_origin))
				except:
					pass
				else:
					block_conditions.append(
						self.bans.c.remote_origin == remote_origin.packed
					)
			if block_conditions:
				conditions.append(or_(*block_conditions))
			else:
				conditions.append(False)
		conditions += string_equal_filter(filter, 'scopes', self.bans.c.scope)
		conditions += string_like_filter(filter, 'reasons', self.bans.c.reason)
		conditions += string_like_filter(filter, 'notes', self.bans.c.note)
		conditions += time_filter(filter, 'expired', self.bans.c.expiration_time)
		conditions += time_filter(filter, 'viewed', self.bans.c.view_time)
		conditions += id_filter(
			filter,
			'created_by_user_ids',
			self.bans.c.created_by_user_id,
		)
		conditions += id_filter(filter, 'user_ids', self.bans.c.user_id)

		statement = self.bans.select()
		if conditions:
			statement = statement.where(and_(*conditions))
		return statement

	def count_bans(self, filter={}):
		statement = self.prepare_bans_search_statement(filter)
		statement = statement.with_only_columns([func.count(self.bans.c.id)])
		return self.connection.execute(statement).fetchone()[0]

	def search_bans(
			self,
			filter={},
			sort='',
			order='',
			page=0,
			perpage=None,
		):
		statement = self.prepare_bans_search_statement(filter)

		statement = sort_statement(
			statement,
			self.bans,
			sort,
			order,
			'creation_time',
			True,
			[
				'creation_time',
				'id',
			],
		)
		statement = paginate_statement(statement, page, perpage)

		result = self.connection.execute(statement).fetchall()
		if 0 == len(result):
			return IDCollection()

		bans = IDCollection()
		for row in result:
			ban = Ban(
				id=row[self.bans.c.id],
				creation_time=row[self.bans.c.creation_time],
				remote_origin=row[self.bans.c.remote_origin],
				scope=row[self.bans.c.scope],
				reason=row[self.bans.c.reason],
				note=row[self.bans.c.note],
				expiration_time=row[self.bans.c.expiration_time],
				view_time=row[self.bans.c.view_time],
				created_by_user_id=row[self.bans.c.created_by_user_id],
				user_id=row[self.bans.c.user_id],
			)

			bans.add(ban)
		return bans

	def check_ban(self, scope='', remote_origin='', user_id=None):
		expiration_conditions = [
			self.bans.c.expiration_time == 0,
			self.bans.c.expiration_time > time.time()
		]
		scope_conditions = [
			self.bans.c.scope == '',
		]
		if scope:
			scope_conditions.append(self.bans.c.scope == scope)

		conditions = []
		if remote_origin:
			conditions.append(
				self.bans.c.remote_origin == ip_address(str(remote_origin)).packed,
			)

		if user_id:
			user_id_bytes = get_id_bytes(user_id)
			conditions.append(self.bans.c.user_id == user_id_bytes)

		if not conditions:
			raise ValueError('Neither remote origin or user id provided')

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

		return Ban(
			id=row[self.bans.c.id],
			creation_time=row[self.bans.c.creation_time],
			remote_origin=row[self.bans.c.remote_origin],
			scope=row[self.bans.c.scope],
			reason=row[self.bans.c.reason],
			note=row[self.bans.c.note],
			expiration_time=row[self.bans.c.expiration_time],
			view_time=row[self.bans.c.view_time],
			created_by_user_id=row[self.bans.c.created_by_user_id],
			user_id=row[self.bans.c.user_id],
		)

	# manipulate bans
	def create_ban(self, **kwargs):
		ban = Ban(**kwargs)
		# preflight check for existing id
		if self.get_ban(id=ban.id_bytes):
			raise ValueError('Ban ID collision')
		self.connection.execute(
			self.bans.insert(),
			id=ban.id_bytes,
			creation_time=int(ban.creation_time),
			remote_origin=ban.remote_origin.packed,
			scope=str(ban.scope),
			reason=str(ban.reason),
			note=str(ban.note),
			expiration_time=int(ban.expiration_time),
			view_time=int(ban.view_time),
			created_by_user_id=ban.created_by_user_id_bytes,
			user_id=ban.user_id_bytes,
		)
		return ban

	def update_ban(self, id, **kwargs):
		ban = Ban(id=id, **kwargs)
		updates = {}
		if 'creation_time' in kwargs:
			updates['creation_time'] = int(ban.creation_time)
		if 'remote_origin' in kwargs:
			updates['remote_origin'] = ban.remote_origin.packed
		if 'scope' in kwargs:
			updates['scope'] = str(ban.scope)
		if 'reason' in kwargs:
			updates['reason'] = str(ban.reason)
		if 'note' in kwargs:
			updates['note'] = str(ban.note)
		if 'expiration_time' in kwargs:
			updates['expiration_time'] = int(ban.expiration_time)
		if 'view_time' in kwargs:
			updates['view_time'] = int(ban.view_time)
		if 'created_by_user_id' in kwargs:
			updates['created_by_user_id'] = ban.created_by_user_id_bytes
		if 'user_id' in kwargs:
			updates['user_id'] = ban.user_id_bytes
		if 0 == len(updates):
			return
		self.connection.execute(
			self.bans.update().values(**updates).where(
				self.bans.c.id == ban.id_bytes
			)
		)

	def delete_ban(self, id):
		id = get_id_bytes(id)
		self.connection.execute(
			self.bans.delete().where(self.bans.c.id == id)
		)

	def prune_bans(self, expired_before=None):
		if not expired_before:
			expired_before = time.time()
		self.connection.execute(
			self.bans.delete().where(
				and_(
					0 != self.bans.c.expiration_time,
					int(expired_before) > self.bans.c.expiration_time,
				)
			)
		)

	def delete_user_bans(self, user_id):
		user_id_bytes = get_id_bytes(user_id)
		self.connection.execute(
			self.bans.delete().where(self.bans.c.user_id == user_id_bytes)
		)

	def anonymize_user_bans(self, user_id, new_user_id=None):
		user_id = get_id_bytes(user_id)

		if not new_user_id:
			new_user_id = uuid.uuid4().bytes

		self.connection.execute(
			self.bans.update().values(user_id=new_user_id).where(
				self.bans.c.user_id == user_id,
			)
		)

	def anonymize_ban_origins(self, bans):
		for ban in bans.values():
			if 4 == ban.remote_origin.version:
				# clear last 16 bits
				anonymized_origin = ip_address(
					int.from_bytes(ban.remote_origin.packed, 'big')
					&~ 0xffff
				)
			elif 6 == ban.remote_origin.version:
				# clear last 80 bits
				anonymized_origin = ip_address(
					int.from_bytes(ban.remote_origin.packed, 'big')
					&~ 0xffffffffffffffffffff
				)
			else:
				raise ValueError('Encountered unknown IP version')
			self.connection.execute(
				self.bans.update().values(
					remote_origin=anonymized_origin.packed
				).where(
					self.bans.c.id == ban.id_bytes
				)
			)
