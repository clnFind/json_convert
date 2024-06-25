# -*- coding: utf-8 -*-

import pymysql
import time


class Mysqldb:
	def __init__(self, db):
		self.db = db
		self.conn, self.cur = self.connectdb()

	def connectdb(self):
		conn, cur = None, None
		flag = 0
		while flag <= 5:
			try:
				conn = pymysql.connect(**self.db)
				cur = conn.cursor()
				break
			except Exception as e:
				time.sleep(5)
				print('mysql connect error:', e)
				flag += 1
				if flag == 5:
					flag = 0
		return conn, cur

	def execute(self, sql, args=None):
		try:
			self.cur.execute(sql, args)
		except Exception as e:
			print('sql is error:', e)
			print('sql is ', sql)
			self.conn, self.cur = self.connectdb()

	def select(self, sql):
		try:
			self.cur.execute(sql)
			res = self.cur.fetchall()
		except Exception as e:
			res = None
			print('select is error :%s' % e)
			print('sql is ', sql)
			self.conn, self.cur = self.connectdb()
		return res

	def executemany(self, sql, datalist):
		try:
			self.cur.executemany(sql, datalist)
			self.commit()
		except Exception as e:
			print('executemany is error,', e)
			print('sql is ', sql)
			print('data is', datalist)
			time.sleep(20)
			self.conn, self.cur = self.connectdb()

	def callproc(self, fx_name, args):
		try:
			self.cur.callproc(fx_name, (args,))
		except Exception as e:
			print('callproc is error,', e)
			print('fx_name is ', fx_name)
			print("args is ", args)
			self.conn, self.cur = self.connectdb()

	def commit(self, ):
		self.conn.commit()

	def close(self, ):
		if self.conn:
			self.cur.close()
			self.conn.close()