import sys
import unittest
import uuid
import time
from datetime import datetime, timezone

from ipaddress import ip_address
from sqlalchemy import create_engine

from base64_url import base64_url_encode, base64_url_decode
from bans import Bans, Ban, parse_id

def compare_int_str_and_bool_attributes(object1, object2):
	# check if all int, string, and bool properties of two objects are equal
	for attr, value in object1.__dict__.items():
		if (
				isinstance(value, int)
				or isinstance(value, str)
				or isinstance(value, bool)
			):
			if value != getattr(object2, attr):
				return False
	return True

db_url = ''

class TestBans(unittest.TestCase):
	def setUp(self):
		if db_url:
			engine = create_engine(db_url)
		else:
			engine = create_engine('sqlite:///:memory:')

		self.bans = Bans(
			engine,
			install=True,
			db_prefix=base64_url_encode(uuid.uuid4().bytes),
		)

	def tearDown(self):
		if db_url:
			self.bans.uninstall()

	def assert_invalid_id_raises(self, f):
		# if id is a string it must be a base64_url string
		# and if id is not a string it must be bytes-like
		for invalid_id in [
				'not a valid base64_url string',
				'invalid_padding_for_base64_url_id',
				1,
				['list'],
				{'dict': 'ionary'},
			]:
			with self.assertRaises(Exception):
				f(invalid_id)

	def assert_invalid_id_returns_none(self, f):
		# if id is a string it must be a base64_url string
		# and if user id is not a string it must be bytes-like
		for invalid_id in [
				'not a valid base64_url string',
				'invalid_padding_for_base64_url_id',
				1,
				[],
				{},
				['list'],
				{'dict': 'ionary'},
			]:
			self.assertEqual(None, f(invalid_id))

	def assert_invalid_timestamp_raises(self, f):
		# anything that doesn't cast to int or that isn't accepted by
		# datetime.fromtimestamp should raise
		for invalid_timestamp in [
				'string',
				['list'],
				{'dict': 'ionary'},
			]:
			with self.assertRaises(Exception):
				f(invalid_timestamp)

	def assert_non_ban_raises(self, f):
		# any non-ban object should raise
		for invalid_ban in [
				'string',
				1,
				['list'],
				{'dict': 'ionary'},
			]:
			with self.assertRaises(Exception):
				f(invalid_ban)

	def assert_invalid_string_raises(self, f):
		# string inputs are cast to string so anything that doesn't
		# cast gracefully to string should raise
		#TODO most objects cast gracefully to a string representation
		#TODO are there python objects that raise when casting to string?
		pass

	def test_parse_id(self):
		for invalid_input in [
				'contains non base64_url characters $%^~',
				['list'],
				{'dict': 'ionary'},
			]:
			with self.assertRaises(Exception):
				id, id_bytes = parse_id(invalid_input)
		expected_bytes = uuid.uuid4().bytes
		expected_string = base64_url_encode(expected_bytes)
		# from bytes
		id, id_bytes = parse_id(expected_bytes)
		self.assertEqual(id_bytes, expected_bytes)
		self.assertEqual(id, expected_string)
		# from string
		id, id_bytes = parse_id(expected_string)
		self.assertEqual(id, expected_string)
		self.assertEqual(id_bytes, expected_bytes)

	# class instantiation, create, get, and defaults
	def class_create_get_and_defaults(self, class_name, create, get, defaults):
		# instantiate directly
		instance = class_name()
		# create in db
		object = create()
		self.assertIsInstance(object, class_name)
		# objects can be retrieved by both id and id_bytes
		self.assertTrue(
			compare_int_str_and_bool_attributes(
				get(object.id),
				get(object.id_bytes),
			)
		)
		# fetched object should be the same as the one returned from create
		self.assertTrue(
			compare_int_str_and_bool_attributes(object, get(object.id))
		)
		# default values should match
		for property, value in defaults.items():
			self.assertEqual(value, getattr(instance, property))
			self.assertEqual(value, getattr(object, property))

	def test_ban_class_create_get_and_defaults(self):
		self.class_create_get_and_defaults(
			Ban,
			self.bans.create_ban,
			self.bans.get_ban,
			{
				'remote_origin': ip_address('192.0.2.0'),
				'scope': '',
				'reason': '',
				'note': '',
				'expiration_time': 0,
				'view_time': 0,
				'created_by_user_id': '',
				'user_id': '',
			},
		)

	#TODO assert properties that default to current time
	#TODO assert properties that default to uuid bytes

	# class instantiation and db object creation with properties
	# id properties
	def id_property(self, class_name, create, property):
		# id can be specified from bytes-like
		expected_id_bytes = uuid.uuid4().bytes
		expected_id = base64_url_encode(expected_id_bytes)
		# instantiate directly
		instance = class_name(**{property: expected_id_bytes})
		instance_id_bytes = getattr(instance, property + '_bytes')
		instance_id = getattr(instance, property)
		self.assertEqual(expected_id_bytes, instance_id_bytes)
		self.assertEqual(expected_id, instance_id)
		# create in db
		object = create(**{property: expected_id_bytes})
		object_id_bytes = getattr(object, property + '_bytes')
		object_id = getattr(object, property)
		self.assertEqual(expected_id_bytes, object_id_bytes)
		self.assertEqual(expected_id, object_id)

		# id can be specified from a base64_url string
		expected_id_bytes = uuid.uuid4().bytes
		expected_id = base64_url_encode(expected_id_bytes)
		# instantiate directly
		instance = class_name(**{property: expected_id})
		instance_id_bytes = getattr(instance, property + '_bytes')
		instance_id = getattr(instance, property)
		self.assertEqual(expected_id_bytes, instance_id_bytes)
		self.assertEqual(expected_id, instance_id)
		# create in db
		object = create(**{property: expected_id})
		object_id_bytes = getattr(object, property + '_bytes')
		object_id = getattr(object, property)
		self.assertEqual(expected_id_bytes, object_id_bytes)
		self.assertEqual(expected_id, object_id)

		self.assert_invalid_id_raises(
			lambda input: class_name(**{property: input})
		)
		self.assert_invalid_id_raises(
			lambda input: create(**{property: input})
		)

	def test_ban_id_property(self):
		self.id_property(Ban, self.bans.create_ban, 'id')

	def test_ban_user_id_property(self):
		self.id_property(Ban, self.bans.create_ban, 'user_id')

	def test_ban_created_by_user_id_property(self):
		self.id_property(Ban, self.bans.create_ban, 'created_by_user_id')

	# time properties
	def time_property(self, class_name, create, property):
		for valid_timestamp in [
				# valid int
				0,
				1111111111,
				1234567890,
				-1,
				# valid but non-int
				0.1,
				1111111111.12345,
				1234567890.12345,
				-1.1,
			]:
			# time properties first cast to int then use datetime.fromtimestamp
			# so shouldn't raise on valid input, but should only result in ints
			valid_timestamp = int(valid_timestamp)
			expected_datetime = datetime.fromtimestamp(
				valid_timestamp,
				timezone.utc,
			)

			# instantiate directly
			instance = class_name(
				**{property + '_time': valid_timestamp}
			)
			instance_time = getattr(instance, property + '_time')
			instance_datetime = getattr(
				instance, property + '_datetime'
			)
			self.assertEqual(valid_timestamp, instance_time)
			self.assertEqual(expected_datetime, instance_datetime)

			# create in db
			object = create(**{property + '_time': valid_timestamp})
			object_time = getattr(object, property + '_time')
			object_datetime = getattr(object, property + '_datetime')
			self.assertEqual(valid_timestamp, object_time)
			self.assertEqual(expected_datetime, object_datetime)

		self.assert_invalid_timestamp_raises(
			lambda input: class_name(**{property + '_time': input})
		)
		self.assert_invalid_timestamp_raises(
			lambda input: create(**{property + '_time': input})
		)

	def test_ban_creation_time_property(self):
		self.time_property(Ban, self.bans.create_ban, 'creation')

	def test_ban_expiration_time_property(self):
		self.time_property(Ban, self.bans.create_ban, 'expiration')

	def test_ban_view_time_property(self):
		self.time_property(Ban, self.bans.create_ban, 'view')

	# string properties
	def string_property(self, class_name, create, property):
		for valid_string in [
				# valid string
				'some string',
				# valid but non-strings
				1,
				0.1,
				[],
				{},
				['list'],
				{'dict': 'ionary'},
			]:
			# string properties first cast to string
			# so shouldn't raise on valid input, but should only result in strings
			valid_string = str(valid_string)

			# instantiate directly
			instance = class_name(
				**{property: valid_string}
			)
			instance_string = getattr(instance, property)
			self.assertEqual(valid_string, instance_string)

			# create in db
			object = create(**{property: valid_string})
			object_string = getattr(object, property)
			self.assertEqual(valid_string, object_string)

		self.assert_invalid_string_raises(
			lambda input: class_name(**{property: input})
		)
		self.assert_invalid_string_raises(
			lambda input: create(**{property: input})
		)

	def test_ban_scope_property(self):
		self.string_property(Ban, self.bans.create_ban, 'scope')

	def test_ban_reason_property(self):
		self.string_property(Ban, self.bans.create_ban, 'reason')

	def test_ban_note_property(self):
		self.string_property(Ban, self.bans.create_ban, 'note')

	# delete
	def delete(self, create, get, delete):
		# by id
		object = create()
		self.assertIsNotNone(get(object.id))
		delete(object.id)
		self.assertIsNone(get(object.id))
		# by id_bytes
		object = create()
		self.assertIsNotNone(get(object.id))
		delete(object.id_bytes)
		self.assertIsNone(get(object.id))

		self.assert_invalid_id_raises(delete)

	def test_delete_ban(self):
		self.delete(
			self.bans.create_ban,
			self.bans.get_ban,
			self.bans.delete_ban,
		)

	# id collision
	def id_collision(self, create):
		object = create()
		# by id
		with self.assertRaises(Exception):
			create(id=object.id)
		# by id_bytes
		with self.assertRaises(Exception):
			create(id=object.id_bytes)

	def test_bans_id_collision(self):
		self.id_collision(self.bans.create_ban)

	# unfiltered count
	def count(self, create, count, delete):
		object1 = create()
		object2 = create()
		self.assertEqual(2, count())

		delete(object2.id)
		self.assertEqual(1, count())

		object3 = create()
		self.assertEqual(2, count())

		delete(object3.id)
		self.assertEqual(1, count())

		delete(object1.id)
		self.assertEqual(0, count())

	def test_count_bans(self):
		self.count(
			self.bans.create_ban,
			self.bans.count_bans,
			self.bans.delete_ban,
		)

	# unfiltered search
	def search(self, create, search, delete):
		object1 = create()
		object2 = create()
		objects = search()
		self.assertTrue(object1 in objects)
		self.assertTrue(object2 in objects)

		delete(object2.id)
		objects = search()
		self.assertTrue(object1 in objects)
		self.assertTrue(object2 not in objects)

		object3 = create()
		objects = search()
		self.assertTrue(object1 in objects)
		self.assertTrue(object2 not in objects)
		self.assertTrue(object3 in objects)

		delete(object3.id)
		objects = search()
		self.assertTrue(object1 in objects)
		self.assertTrue(object2 not in objects)
		self.assertTrue(object3 not in objects)

		delete(object1.id)
		objects = search()
		self.assertTrue(object1 not in objects)
		self.assertTrue(object2 not in objects)
		self.assertTrue(object3 not in objects)

	def test_search_bans(self):
		self.search(
			self.bans.create_ban,
			self.bans.search_bans,
			self.bans.delete_ban,
		)

	# sort order and pagination
	def search_sort_order_and_pagination(
			self,
			create,
			column_field,
			search,
			numeric=True,
		):
		first_kwargs = {}
		middle_kwargs = {}
		last_kwargs = {}
		if numeric:
			first_kwargs[column_field] = 1
			middle_kwargs[column_field] = 2
			last_kwargs[column_field] = 3
		else:
			first_kwargs[column_field] = 'a'
			middle_kwargs[column_field] = 'b'
			last_kwargs[column_field] = 'c'

		object_first = create(**first_kwargs)
		object_middle = create(**middle_kwargs)
		object_last = create(**last_kwargs)

		# ascending
		ascending_objects = [
			object_first,
			object_middle,
			object_last,
		]
		objects = search(sort=column_field, order='asc')
		for object in ascending_objects:
			self.assertTrue(
				compare_int_str_and_bool_attributes(
					object,
					objects.values()[ascending_objects.index(object)],
				)
			)
		for page in range(4):
			objects = search(sort=column_field, order='asc', perpage=1, page=page)
			for object in ascending_objects:
				if ascending_objects.index(object) != page:
					self.assertTrue(object not in objects)
				else:
					self.assertTrue(object in objects)

		# descending
		descending_objects = [
			object_last,
			object_middle,
			object_first,
		]
		objects = search(sort=column_field, order='desc')
		for object in descending_objects:
			self.assertTrue(
				compare_int_str_and_bool_attributes(
					object,
					objects.values()[descending_objects.index(object)],
				)
			)
		for page in range(4):
			objects = search(sort=column_field, order='desc', perpage=1, page=page)
			for object in descending_objects:
				if descending_objects.index(object) != page:
					self.assertTrue(object not in objects)
				else:
					self.assertTrue(object in objects)

	def test_search_bans_creation_time_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.bans.create_ban,
			'creation_time',
			self.bans.search_bans,
			numeric=True,
		)

	def test_search_bans_scope_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.bans.create_ban,
			'scope',
			self.bans.search_bans,
			numeric=False,
		)

	def test_search_bans_reason_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.bans.create_ban,
			'reason',
			self.bans.search_bans,
			numeric=False,
		)

	def test_search_bans_note_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.bans.create_ban,
			'note',
			self.bans.search_bans,
			numeric=False,
		)

	def test_search_bans_expiration_time_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.bans.create_ban,
			'expiration_time',
			self.bans.search_bans,
			numeric=True,
		)

	def test_search_bans_view_time_sort_order_and_pagination(self):
		self.search_sort_order_and_pagination(
			self.bans.create_ban,
			'view_time',
			self.bans.search_bans,
			numeric=True,
		)

	# search by id
	def search_by_id(self, create, search):
		object1 = create()
		object2 = create()

		objects = search(
			filter={'ids': object1.id}
		)
		self.assertTrue(object1 in objects)
		self.assertTrue(object2 not in objects)

		objects = search(
			filter={'ids': object2.id}
		)
		self.assertTrue(object1 not in objects)
		self.assertTrue(object2 in objects)

		objects = search(
			filter={
				'ids': [
					object1.id,
					object2.id,
				]
			}
		)
		self.assertTrue(object1 in objects)
		self.assertTrue(object2 in objects)

		invalid_values = [
			'not a valid base64_url string',
			'invalid_padding_for_base64_url_id',
			1,
		]
		# filters with all invalid values should return none
		objects = search(filter={'ids': invalid_values})
		self.assertEqual(0, len(objects))
		for invalid_value in invalid_values:
			objects = search(filter={'ids': invalid_value})
			self.assertEqual(0, len(objects))
		# filters with at least one valid value should behave normally
		# ignoring any invalid values
		objects = search(filter={'ids': invalid_values + [object1.id]})
		self.assertTrue(object1 in objects)
		self.assertTrue(object2 not in objects)
		for invalid_value in invalid_values:
			objects = search(filter={'ids': [invalid_value, object1.id]})
			self.assertTrue(object1 in objects)
			self.assertTrue(object2 not in objects)

	def test_search_bans_by_id(self):
		self.search_by_id(self.bans.create_ban, self.bans.search_bans)

	# search by user id
	def search_by_user_id(
			self,
			create,
			column_field,
			search,
			filter_field,
		):
		user1_id = uuid.uuid4().bytes
		user2_id = uuid.uuid4().bytes
		user3_id = uuid.uuid4().bytes

		object1 = create(**{column_field: user1_id})
		object2 = create(**{column_field: user2_id})

		objects = search(filter={filter_field: user1_id})
		self.assertTrue(object1 in objects)
		self.assertTrue(object2 not in objects)

		objects = search(filter={filter_field: user2_id})
		self.assertTrue(object1 not in objects)
		self.assertTrue(object2 in objects)

		objects = search(filter={filter_field: user3_id})
		self.assertEqual(0, len(objects))

		objects = search(filter={filter_field: [user1_id, user2_id]})
		self.assertTrue(object1 in objects)
		self.assertTrue(object2 in objects)

		invalid_values = [
			'not a valid base64_url string',
			'invalid_padding_for_base64_url_id',
			1,
		]
		# filters with all invalid values should return none
		objects = search(filter={filter_field: invalid_values})
		self.assertEqual(0, len(objects))
		for invalid_value in invalid_values:
			objects = search(filter={filter_field: invalid_value})
			self.assertEqual(0, len(objects))
		# filters with at least one valid value should behave normally
		# ignoring any invalid values
		objects = search(filter={filter_field: invalid_values + [user1_id]})
		self.assertTrue(object1 in objects)
		self.assertTrue(object2 not in objects)
		for invalid_value in invalid_values:
			objects = search(filter={filter_field: [invalid_value, user1_id]})
			self.assertTrue(object1 in objects)
			self.assertTrue(object2 not in objects)

	def test_search_bans_by_created_by_user_id(self):
		self.search_by_user_id(
			self.bans.create_ban,
			'created_by_user_id',
			self.bans.search_bans,
			'created_by_user_ids',
		)

	def test_search_bans_by_user_id(self):
		self.search_by_user_id(
			self.bans.create_ban,
			'user_id',
			self.bans.search_bans,
			'user_ids',
		)

	# search by time
	def search_by_time(
			self,
			create,
			column_field,
			search,
			filter_field,
		):
		time_oldest = 0
		time_middle = 1
		time_newest = 2

		object_oldest = create(**{column_field: time_oldest})
		object_middle = create(**{column_field: time_middle})
		object_newest = create(**{column_field: time_newest})

		objects = search(
			filter={filter_field + '_before': time_newest}
		)
		self.assertEqual(2, len(objects))
		self.assertTrue(object_oldest in objects)
		self.assertTrue(object_middle in objects)
		self.assertTrue(object_newest not in objects)

		objects = search(
			filter={filter_field + '_before': time_middle}
		)
		self.assertEqual(1, len(objects))
		self.assertTrue(object_oldest in objects)
		self.assertTrue(object_middle not in objects)
		self.assertTrue(object_newest not in objects)

		objects = search(filter={filter_field + '_before': time_oldest})
		self.assertEqual(0, len(objects))

		objects = search(filter={filter_field + '_after': time_oldest})
		self.assertEqual(2, len(objects))
		self.assertTrue(object_oldest not in objects)
		self.assertTrue(object_middle in objects)
		self.assertTrue(object_newest in objects)

		objects = search(
			filter={filter_field + '_after': time_middle}
		)
		self.assertEqual(1, len(objects))
		self.assertTrue(object_oldest not in objects)
		self.assertTrue(object_middle not in objects)
		self.assertTrue(object_newest in objects)

		objects = search(
			filter={filter_field + '_after': time_newest}
		)
		self.assertEqual(0, len(objects))

		objects = search(
			filter={
				filter_field + '_after': time_oldest,
				filter_field + '_before': time_newest,
			}
		)
		self.assertEqual(1, len(objects))
		self.assertTrue(object_oldest not in objects)
		self.assertTrue(object_middle in objects)
		self.assertTrue(object_newest not in objects)

		# time filters are cast to int before query
		# so anything that doesn't cast to int should raise
		invalid_times = ['string', b'']
		for invalid_time in invalid_times:
			for field_suffix in ['_before', '_after']:
				with self.assertRaises(ValueError):
					search(filter={filter_field + field_suffix: invalid_time})

	def search_bans_by_creation_time(self):
		self.search_by_time(
			self.bans.create_ban,
			'creation_time',
			self.bans.search_bans,
			'created',
		)

	def search_bans_by_expiration_time(self):
		self.search_by_time(
			self.bans.create_ban,
			'expiration_time',
			self.bans.search_bans,
			'expired',
		)

	def search_bans_by_view_time(self):
		self.search_by_time(
			self.bans.create_ban,
			'view_time',
			self.bans.search_bans,
			'viewed',
		)

	# search by string like
	def search_by_string_like(
			self,
			create,
			column_field,
			search,
			filter_field,
		):
		object_foo = create(**{column_field: 'foo'})
		object_bar = create(**{column_field: 'bar'})
		object_baz = create(**{column_field: 'baz'})

		objects = search(filter={filter_field: 'foo'})
		self.assertTrue(object_foo in objects)
		self.assertTrue(object_bar not in objects)
		self.assertTrue(object_baz not in objects)

		objects = search(filter={filter_field: 'bar'})
		self.assertTrue(object_foo not in objects)
		self.assertTrue(object_bar in objects)
		self.assertTrue(object_baz not in objects)

		objects = search(filter={filter_field: 'ba%'})
		self.assertTrue(object_foo not in objects)
		self.assertTrue(object_bar in objects)
		self.assertTrue(object_baz in objects)

		objects = search(filter={filter_field: 'bat'})
		self.assertEqual(0, len(objects))

		objects = search(filter={filter_field: ['foo', 'bar']})
		self.assertTrue(object_foo in objects)
		self.assertTrue(object_bar in objects)

		# filters with all invalid values should return none
		# filters with at least one valid value should behave normally
		# ignoring any invalid values
		# but since filters are cast to string before the query they should
		# always be valid
		pass

	def test_search_bans_by_reason(self):
		self.search_by_string_like(
			self.bans.create_ban,
			'reason',
			self.bans.search_bans,
			'reasons',
		)

	def test_search_bans_by_note(self):
		self.search_by_string_like(
			self.bans.create_ban,
			'note',
			self.bans.search_bans,
			'notes',
		)

	# search by string equal
	def search_by_string_equal(
			self,
			create,
			column_field,
			search,
			filter_field,
		):
		object_foo = create(**{column_field: 'foo'})
		object_bar = create(**{column_field: 'bar'})
		object_baz = create(**{column_field: 'baz'})

		objects = search(filter={filter_field: 'foo'})
		self.assertTrue(object_foo in objects)
		self.assertTrue(object_bar not in objects)
		self.assertTrue(object_baz not in objects)

		objects = search(filter={filter_field: 'bar'})
		self.assertTrue(object_foo not in objects)
		self.assertTrue(object_bar in objects)
		self.assertTrue(object_baz not in objects)

		objects = search(filter={filter_field: 'baz'})
		self.assertTrue(object_foo not in objects)
		self.assertTrue(object_bar not in objects)
		self.assertTrue(object_baz in objects)

		objects = search(filter={filter_field: 'bat'})
		self.assertEqual(0, len(objects))

		# filters with all invalid values should return none
		# filters with at least one valid value should behave normally
		# ignoring any invalid values
		# but since filters are cast to string before the query they should
		# always be valid
		pass

	def test_search_bans_by_scope(self):
		self.search_by_string_equal(
			self.bans.create_ban,
			'scope',
			self.bans.search_bans,
			'scopes',
		)

	# ban
	def test_update_ban(self):
		# update_ban instantiates a Ban object so anything that raises in
		# test_ban_class_create_get_and_defaults should raise
		ban = self.bans.create_ban()

		# update_ban can receive a base64_url string
		properties = {
			'creation_time': 1111111111,
			'remote_origin': ip_address('1.2.3.4'),
			'scope': 'scope1',
			'reason': 'Reason1',
			'note': 'note1',
			'expiration_time': 2222222222,
			'view_time': 1234567890,
			'user_id': base64_url_encode(uuid.uuid4().bytes),
			'created_by_user_id': base64_url_encode(uuid.uuid4().bytes),
		}
		self.bans.update_ban(ban.id, **properties)
		ban = self.bans.get_ban(ban.id_bytes)
		for key, value in properties.items():
			self.assertEqual(getattr(ban, key), value)

		# update_ban can receive bytes-like
		properties = {
			'creation_time': 2222222222,
			'remote_origin': ip_address('2.3.4.5'),
			'scope': 'scope2',
			'reason': 'Reason2',
			'note': 'note2',
			'expiration_time': 3456789012,
			'view_time': 2345678901,
			'user_id': base64_url_encode(uuid.uuid4().bytes),
			'created_by_user_id': base64_url_encode(uuid.uuid4().bytes),
		}
		self.bans.update_ban(ban.id_bytes, **properties)
		ban = self.bans.get_ban(ban.id_bytes)
		for key, value in properties.items():
			self.assertEqual(getattr(ban, key), value)

		self.assert_invalid_id_raises(self.bans.update_ban)

	def test_prune_bans_all(self):
		ban1 = self.bans.create_ban(expiration_time=time.time() + 1000)
		ban2 = self.bans.create_ban(expiration_time=1)
		ban3 = self.bans.create_ban(expiration_time=2)
		ban4 = self.bans.create_ban(expiration_time=3)

		self.assertIsNotNone(self.bans.get_ban(ban1.id))
		self.assertIsNotNone(self.bans.get_ban(ban2.id))
		self.assertIsNotNone(self.bans.get_ban(ban3.id))
		self.assertIsNotNone(self.bans.get_ban(ban4.id))

		self.bans.prune_bans()

		# unexpired bans aren't pruned
		self.assertIsNotNone(self.bans.get_ban(ban1.id))

		self.assertIsNone(self.bans.get_ban(ban2.id))
		self.assertIsNone(self.bans.get_ban(ban3.id))
		self.assertIsNone(self.bans.get_ban(ban4.id))

	def test_prune_bans_expired_before(self):
		ban1 = self.bans.create_ban(expiration_time=time.time() + 1000)
		ban2 = self.bans.create_ban(expiration_time=1)
		ban3 = self.bans.create_ban(expiration_time=2)
		ban4 = self.bans.create_ban(expiration_time=3)

		self.assertIsNotNone(self.bans.get_ban(ban1.id))
		self.assertIsNotNone(self.bans.get_ban(ban2.id))
		self.assertIsNotNone(self.bans.get_ban(ban3.id))
		self.assertIsNotNone(self.bans.get_ban(ban4.id))

		self.bans.prune_bans(expired_before=3)

		# unexpired bans aren't pruned
		self.assertIsNotNone(self.bans.get_ban(ban1.id))

		self.assertIsNone(self.bans.get_ban(ban2.id))
		self.assertIsNone(self.bans.get_ban(ban3.id))
		self.assertIsNotNone(self.bans.get_ban(ban4.id))

		self.assert_invalid_timestamp_raises(
			lambda input: self.bans.prune_bans(expired_before=input)
		)

	def test_delete_user_bans(self):
		user_id_bytes = uuid.uuid4().bytes
		user_id = base64_url_encode(uuid.uuid4().bytes)

		# by id
		ban1 = self.bans.create_ban(user_id=user_id)
		ban2 = self.bans.create_ban(user_id=user_id)
		self.assertIsNotNone(self.bans.get_ban(ban1.id))
		self.assertIsNotNone(self.bans.get_ban(ban2.id))

		self.bans.delete_user_bans(user_id)

		self.assertIsNone(self.bans.get_ban(ban1.id))
		self.assertIsNone(self.bans.get_ban(ban2.id))

		# by id_bytes
		ban1 = self.bans.create_ban(user_id=user_id)
		ban2 = self.bans.create_ban(user_id=user_id)
		self.assertIsNotNone(self.bans.get_ban(ban1.id))
		self.assertIsNotNone(self.bans.get_ban(ban2.id))

		self.bans.delete_user_bans(user_id)

		self.assertIsNone(self.bans.get_ban(ban1.id))
		self.assertIsNone(self.bans.get_ban(ban2.id))

		self.assert_invalid_id_raises(self.bans.delete_user_bans)

	def test_search_bans_by_remote_origin(self):
		remote_origin1 = '1.1.1.1'
		remote_origin2 = '2.2.2.2'
		ban1 = self.bans.create_ban(remote_origin=remote_origin1)
		ban2 = self.bans.create_ban(remote_origin=remote_origin1)
		ban3 = self.bans.create_ban(remote_origin=remote_origin2)

		bans = self.bans.search_bans(
			filter={'remote_origins': remote_origin1},
		)
		self.assertTrue(ban1 in bans)
		self.assertTrue(ban2 in bans)
		self.assertTrue(ban3 not in bans)

		bans = self.bans.search_bans(
			filter={'remote_origins': remote_origin2},
		)
		self.assertTrue(ban1 not in bans)
		self.assertTrue(ban2 not in bans)
		self.assertTrue(ban3 in bans)

		bans = self.bans.search_bans(
			filter={'remote_origins': [remote_origin1, remote_origin2]},
		)
		self.assertTrue(ban1 in bans)
		self.assertTrue(ban2 in bans)
		self.assertTrue(ban3 in bans)

		invalid_values = [
			'not a valid ip address string',
			['list'],
			{'dict': 'ionary'},
		]
		# filters with all invalid values should return none
		bans = self.bans.search_bans(
			filter={'remote_origins': invalid_values}
		)
		self.assertEqual(0, len(bans))
		for invalid_value in invalid_values:
			bans = self.bans.search_bans(
				filter={'remote_origins': invalid_value},
			)
			self.assertEqual(0, len(bans))
		# filters with at least one valid value should behave normally
		# ignoring any invalid values
		bans = self.bans.search_bans(
			filter={'remote_origins': invalid_values + [remote_origin1]},
		)
		self.assertTrue(ban1 in bans)
		self.assertTrue(ban2 in bans)
		self.assertTrue(ban3 not in bans)
		for invalid_value in invalid_values:
			bans = self.bans.search_bans(
				filter={'remote_origins': [invalid_value, remote_origin1]},
			)
			self.assertTrue(ban1 in bans)
			self.assertTrue(ban2 in bans)
			self.assertTrue(ban3 not in bans)

	def test_check_ban(self):
		origin1 = '1.2.3.4'
		origin2 = '2.3.4.5'
		origin3 = '3.4.5.6'

		user1_id = uuid.uuid4().bytes
		user2_id = uuid.uuid4().bytes
		user3_id = uuid.uuid4().bytes

		expiration_time = time.time() + 1000

		ban1 = self.bans.create_ban(
			remote_origin=origin1,
			scope='scope',
			expiration_time=expiration_time,
			user_id=user1_id,
		)
		ban2 = self.bans.create_ban(
			remote_origin=origin2,
			scope='scope',
			expiration_time=expiration_time,
			user_id=user2_id,
		)

		# by remote origin
		self.assertTrue(
			compare_int_str_and_bool_attributes(
				ban1,
				self.bans.check_ban(scope='scope', remote_origin=origin1),
			)
		)
		self.assertTrue(
			compare_int_str_and_bool_attributes(
				ban2,
				self.bans.check_ban(scope='scope', remote_origin=origin2),
			)
		)
		self.assertIsNone(
			self.bans.check_ban(scope='scope', remote_origin=origin3)
		)
		# by user id
		self.assertTrue(
			compare_int_str_and_bool_attributes(
				ban1,
				self.bans.check_ban(scope='scope', user_id=user1_id),
			)
		)
		self.assertTrue(
			compare_int_str_and_bool_attributes(
				ban2,
				self.bans.check_ban(scope='scope', user_id=user2_id),
			)
		)
		self.assertIsNone(
			self.bans.check_ban(scope='scope', user_id=user3_id)
		)

	def test_check_ban_with_global(self):
		origin1 = '1.2.3.4'
		origin2 = '2.3.4.5'
		origin3 = '3.4.5.6'

		user1_id = uuid.uuid4().bytes
		user2_id = uuid.uuid4().bytes
		user3_id = uuid.uuid4().bytes

		expiration_time = time.time() + 1000

		ban1 = self.bans.create_ban(
			remote_origin=origin1,
			scope='',
			expiration_time=expiration_time,
			user_id=user1_id,
		)
		ban2 = self.bans.create_ban(
			remote_origin=origin2,
			scope='',
			expiration_time=expiration_time,
			user_id=user2_id,
		)

		# check_ban for scope returns global ban for same user id/origin
		# by remote origin
		self.assertTrue(
			compare_int_str_and_bool_attributes(
				ban1,
				self.bans.check_ban(scope='scope', remote_origin=origin1),
			)
		)
		self.assertTrue(
			compare_int_str_and_bool_attributes(
				ban2,
				self.bans.check_ban(scope='scope', remote_origin=origin2),
			)
		)
		# by user id
		self.assertTrue(
			compare_int_str_and_bool_attributes(
				ban1,
				self.bans.check_ban(scope='scope', user_id=user1_id),
			)
		)
		self.assertTrue(
			compare_int_str_and_bool_attributes(
				ban2,
				self.bans.check_ban(scope='scope', user_id=user2_id),
			)
		)

	def test_check_ban_returns_longest(self):
		expiration_time = time.time() + 1000

		user_id = uuid.uuid4().bytes
		ban1 = self.bans.create_ban(
			expiration_time=expiration_time,
			user_id=user_id,
		)
		ban2 = self.bans.create_ban(
			expiration_time=(expiration_time + 1),
			user_id=user_id,
		)
		self.assertTrue(
			compare_int_str_and_bool_attributes(
				ban2,
				self.bans.check_ban(user_id=user_id),
			)
		)

	# anonymization
	def test_anonymize_user(self):
		user_id = base64_url_encode(uuid.uuid4().bytes)
		ban = self.bans.create_ban(user_id=user_id)

		self.assertIsNotNone(self.bans.get_ban(ban.id))
		self.assertEqual(user_id, self.bans.get_ban(ban.id).user_id)

		new_id_bytes = self.bans.anonymize_user(user_id)

		self.assertEqual(0, self.bans.count_bans(filter={'user_ids': user_id}))

		# assert bans still exist, but with the new user id
		self.assertIsNotNone(
			self.bans.search_bans(filter={'user_ids': new_id_bytes})
		)
		self.assertEqual(
			1,
			self.bans.count_bans(filter={'user_ids': new_id_bytes}),
		)
		self.assertNotEqual(user_id, self.bans.get_ban(ban.id).user_id)

	def test_anonymize_ban_origins(self):
		origin1 = '1.2.3.4'
		expected_anonymized_origin1 = '1.2.0.0'
		ban1 = self.bans.create_ban(remote_origin=origin1)

		origin2 = '2001:0db8:85a3:0000:0000:8a2e:0370:7334'
		expected_anonymized_origin2 = '2001:0db8:85a3:0000:0000:0000:0000:0000'
		ban2 = self.bans.create_ban(remote_origin=origin2)

		bans = self.bans.search_bans()
		self.bans.anonymize_ban_origins(bans)

		anonymized_ban1 = self.bans.get_ban(ban1.id)
		anonymized_ban2 = self.bans.get_ban(ban2.id)

		self.assertEqual(
			expected_anonymized_origin1,
			anonymized_ban1.remote_origin.exploded,
		)
		self.assertEqual(
			expected_anonymized_origin2,
			anonymized_ban2.remote_origin.exploded,
		)

if __name__ == '__main__':
	if '--db' in sys.argv:
		index = sys.argv.index('--db')
		if len(sys.argv) - 1 <= index:
			print('missing db url, usage:')
			print(' --db "dialect://user:password@server"')
			quit()
		db_url = sys.argv[index + 1]
		print('using specified db: "' + db_url + '"')
		del sys.argv[index:]
	else:
		print('using sqlite:///:memory:')
	print(
		'use --db [url] to test with specified db url'
			+ ' (e.g. sqlite:///bans_tests.db)'
	)
	unittest.main()
