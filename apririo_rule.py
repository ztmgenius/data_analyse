#!/usr/local/bin/python3
# coding=utf-8
"""
	店铺相关性分析：提取近一年的数据，分析两个店铺的支持度、置信度、提升度、kulc、IR值，最后使用IR值作为相关性判断，值越大关联关系越强
"""
import pandas as pd
import sys
# 注意这个也要import一次
import matplotlib.pyplot as plt
import datetime
from dateutil.relativedelta import relativedelta
from time import strftime, localtime
import apyori
import csv
import time
from multiprocessing import Pool
import networkx as nx
import psycopg2
import os

import common
import upload_file


def mall_apriori_test(mid):
	start_time = datetime.datetime.now()

	cmd = connect_db + '\"COPY (select id,sname from ods_shop where mid=' + mid + ' ) TO \'' + work_dir + \
		  'order_data_shop_' + mid + '.csv\'  WITH csv header \"'
	returncode, out = common.execute_cmd(cmd)
	if returncode != 0:
		raise SystemExit('execute {0} err :{1}'.format(cmd, out))
	df_shop = pd.read_csv(work_dir + 'order_data_shop_' + mid + '.csv', index_col=False)
	os.remove(work_dir + 'order_data_shop_' + mid + '.csv')
	# 循环内部针对列表元素进行操作
	logger.info("{0} Mall:{1} processing ...".format("=" * 30, mid))
	for rule in rule_type:
		rule = [40000013, '零售+餐饮', 0.001, 0.005, .01]
		# 循环内部针对列表元素进行操作
		logger.info("\tmid:{0} {1} processing ...".format(mid, rule))
		# 导出数据
		begin_date = datetime.datetime.now()
		begin_date = pd.to_datetime((begin_date - relativedelta(months=+12)).date())
		begin_date = begin_date.strftime('%Y-%m-%d')
		end_date = strftime('%Y-%m-%d', localtime())
		# begin_date='2020-01-01'
		# end_date='2021-01-01'

		# 从表取数
		rule_cond = ' (prm_category_1 = 40000013 or prm_category_1 = 40000237) and biz_type!=9 and '
		exclude_sid = common.get_sys_var("environment variables", "rfm_exclude_sid")
		cmd = connect_db + f""" "COPY ( 
			select a.sid sid from 
			( select b.gid,array_to_string(array_agg(b.sid),',') as sid from  
				( select gid,sid from mall{mid}.dwd_order_with_category where {rule_cond} sid<> 20442 and sid<> 17745 and sid<> 11661 and     
					to_char(trade_date,'yyyy-mm-dd')>='{begin_date}' and to_char(trade_date,'yyyy-mm-dd')<'{end_date}' {exclude_sid} group by gid,sid 
				) b  group by b.gid 
			) a  
			) TO '{work_dir}order_data_apri_{mid}.csv' " """  # WITH csv header

		returncode, out = common.execute_cmd(cmd)
		if returncode != 0:
			raise SystemExit('execute {0} err :{1}'.format(cmd, out))

		data = []
		with open(work_dir + 'order_data_apri_' + mid + '.csv', encoding='utf8') as f:
			reader = csv.reader(f)  # 加载csv
			# headers = next(reader)  # 第一行，即表头
			# print(headers)
			for row in reader:
				data.append(row)
		# data = [["豆奶", "莴苣"],["莴苣", "尿布", "啤酒", "甜菜"],["豆奶", "尿布", "啤酒", "橙汁"],	["莴苣", "豆奶", "尿布", "啤酒"],
		# ["莴苣", "豆奶", "尿布", "橙汁"]]
		os.remove(work_dir + 'order_data_apri_' + mid + '.csv')

		ctime = strftime('%Y-%m-%d %H:%M:%S', localtime())
		if len(data) == 0:
			logger.info("\t\tmid:{0} 0 条记录".format(mid))
			continue

		result = apyori.apriori(transactions=data, min_support=rule[2], min_confidence=rule[3], min_lift=rule[4], max_length=2)
		"""
		items – 项集，frozenset对象，可迭代取出子集。
		support – 支持度，float类型。
		confidence – 置信度或可信度， float类型。
		ordered_statistics – 存在的关联规则
		可迭代，迭代后，其元素的属性：
		items_base – 关联规则中的分母项集
		confidence – 上面的分母规则所对应的关联规则的可信度
		"""
		logger.info("\t\tmid:{0} 总人数:{1}".format(mid, len(data)))
		lines = []
		for i in result:
			lists_item = list(i.items)
			for j in i.ordered_statistics:
				lists_base = list(j.items_base)
				lists_add = list(j.items_add)
				if lists_base != []:
					line = []
					line.append(mid)
					line.append(rule[0])
					line.append(rule[1])
					line.append(' '.join(lists_item))
					line.append(i.support)
					line.append(' '.join(lists_base))
					line.append(' '.join(lists_add))
					line.append(j.confidence)
					line.append(j.lift)
					line.append(0)
					line.append(0)
					line.append(ctime)
					lines.append(line)

		"""
		k=0
		#按支持度取前max_rule_num条
		lines.sort(key = lambda x:x[4], reverse=True)
		with open(work_dir+'result_data_apri_'+mid+'.csv', 'w') as f:
			s='mid,rule_type,rule_name,rule_set,support,rule_set1,rule_set2,confidence,lift,kulc,ir,ctime\n'
			f.write(s)
			for i in lines:
				s='%s,%d,%s,%s,%.4f,%s,%s,%.4f,%.4f,%d,%d,%s\n' % (i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9],i[10],i[11])
				if k < max_rule_num :
					f.write(s)
					k+=1
				else :
					break
		print("\t\tmid:",mid,"关联规则数:",k)
		"""
		# 按kulc值取数据
		if len(lines) == 0:
			logger.info("\t\tmid:{0} 关联规则数:{1}".format(mid, len(lines)))
			continue
		data = pd.DataFrame(lines)
		data.rename(columns={0: 'mid', 1: 'rule_type', 2: 'rule_name', 3: 'rule_set', 4: 'support', 5: 'rule_set1',
							 6: 'rule_set2', 7: 'confidence', 8: 'lift', 9: 'kulc', 10: 'ir', 11: 'ctime'},
					inplace=True)
		conf1 = 0
		conf2 = 0
		rule_set1 = ''
		rule_set2 = ''
		data = data[data['rule_set1'] > '0']
		data = data[data['rule_set2'] > '0']
		for i in range(0, len(data)):
			if i == 0:
				conf1 = data.iloc[i]['confidence']
				rule_set1 = data.iloc[i]['rule_set']
				continue
			conf2 = data.iloc[i]['confidence']
			rule_set2 = data.iloc[i]['rule_set']
			if rule_set1 == rule_set2:
				data.iloc[i, 9] = (conf1 + conf2) / 2
				data.iloc[i - 1, 9] = (conf1 + conf2) / 2
				data.iloc[i, 10] = conf2 / conf1
				data.iloc[i - 1, 10] = conf1 / conf2
			conf1 = conf2
			rule_set1 = rule_set2
		data = data[data['kulc'] > 0]
		data = data.sort_values(by=["kulc", "rule_set"], ascending=(False))
		data.reset_index(drop=True)
		if len(data) > max_rule_num:
			data = data.iloc[:max_rule_num]
		# data.reset_index(drop = True)
		logger.info("\t\tmid:{0} 关联规则数: {1}".format(mid, len(data)))
		data.to_csv(work_dir + 'result_data_apri_' + mid + '.csv', encoding='utf_8_sig', index=False, sep=",")
		cmd = connect_db + '\"COPY public.dwa_association_rule FROM \'' + work_dir + 'result_data_apri_' + mid + '.csv\' WITH csv header DELIMITER \',\' ENCODING \'utf8\' \"'
		returncode, out = common.execute_cmd(cmd)
		if returncode != 0:
			raise SystemExit('execute {0} err :{1}'.format(cmd, out))

		# python pandas 如何去掉/保留数据集中的重复行？
		data = data.sort_values(by=['mid', 'rule_type', 'rule_set', 'ir'], ascending=(True, True, True, False))
		data = data.drop_duplicates(subset=['mid', 'rule_type', 'rule_set'], keep='first')
		data = data.sort_values(by=['mid', 'rule_type', 'ir'], ascending=(True, True, False))
		if len(data) > 30:
			data = data.iloc[:30]
		if len(data) < 10:
			continue
		try:
			data['width'] = pd.qcut(data['ir'], q=[0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1], precision=2,
								labels=[.1, .2, .25, .3, .35, .4, .45, 0.6, 1.0, 2])
		except:
			continue
		# df_shop['id'] = df_shop['id'].astype('object')
		data['rule_set1'] = data['rule_set1'].astype('int64')
		data['rule_set2'] = data['rule_set2'].astype('int64')
		data = pd.merge(data, df_shop, left_on='rule_set1', right_on='id', how='left')
		data = pd.merge(data, df_shop, left_on='rule_set2', right_on='id', how='left')

		# 绘制网络图https://oomake.com/question/3891541
		net_grid = nx.DiGraph()
		pos = nx.circular_layout(net_grid)
		for i in range(0, len(data)):
			n1 = data.iloc[i]['sname_x']
			n2 = data.iloc[i]['sname_y']
			n3 = data.iloc[i]['width']
			net_grid.add_node(n1)
			net_grid.add_node(n2)
			net_grid.add_edge(n1, n2, width=n3)
		nx.draw_networkx_nodes(net_grid, pos=nx.circular_layout(net_grid), node_color='#ff0000', node_size=10,
							   alpha=0.6)  # 点的样式
		for i in range(0, len(data)):
			n1 = data.iloc[i]['sname_x']
			n2 = data.iloc[i]['sname_y']
			n3 = data.iloc[i]['width']
			nx.draw_networkx_edges(net_grid, pos=nx.circular_layout(net_grid), edgelist=[(n1, n2)], width=n3,
								   edge_color='b')
		nx.draw_networkx_labels(net_grid, pos=nx.circular_layout(net_grid), font_size=6, font_color='k', alpha=0.6)
		plt.title(mid + '.' + rule[1] + "店铺关联关系")
		plt.rcParams['font.family'] = ['sans-serif']
		plt.rcParams['font.sans-serif'] = ['SimSun']
		file_name = work_dir + 'png/apri_' + mid + '_' + str(rule[0]) + '_' + strftime('%Y-%m-%d', localtime()) + '.png'
		plt.savefig(file_name, dpi=200)
		plt.show()
		plt.close('all')

		os.remove(work_dir + 'result_data_apri_' + mid + '.csv')
		logger.info("\t\tmid:{0} Duration {1}".format(mid, datetime.datetime.now() - start_time))
		break


def mall_apriori(mid):
	start_time = datetime.datetime.now()
	cmd = connect_db + '\"delete from public.dwa_association_rule where to_char(ctime,\'yyyy-mm-dd\')=\'' + strftime(
		'%Y-%m-%d', localtime()) + '\' and mid=' + mid + '\"'
	returncode, out = common.execute_cmd(cmd)
	cmd = connect_db + '\"COPY (select id,sname from ods_shop where mid=' + mid + ' ) TO \'' + work_dir + \
		  'order_data_shop_' + mid + '.csv\'  WITH csv header \"'
	returncode, out = common.execute_cmd(cmd)
	if returncode != 0:
		raise SystemExit('execute {0} err :{1}'.format(cmd, out))
	df_shop = pd.read_csv(work_dir + 'order_data_shop_' + mid + '.csv', index_col=False)
	os.remove(work_dir + 'order_data_shop_' + mid + '.csv')
	# 循环内部针对列表元素进行操作
	logger.info("{0} Mall:{1} processing ...".format("=" * 30, mid))
	for rule in rule_type:
		# 循环内部针对列表元素进行操作
		logger.info("\tmid:{0} {1} processing ...".format(mid, rule))
		# 导出数据
		begin_date = datetime.datetime.now()
		begin_date = pd.to_datetime((begin_date - relativedelta(months=+12)).date())
		begin_date = begin_date.strftime('%Y-%m-%d')
		end_date = strftime('%Y-%m-%d', localtime())
		# begin_date='2020-01-01'
		# end_date='2021-01-01'

		# 从表取数
		if rule[0] == 0:
			rule_cond = ' biz_type<>9 and '
		else:
			rule_cond = ' prm_category_1 =' + str(rule[0]) + ' and biz_type<>9 and '
		exclude_sid = common.get_sys_var("environment variables", "rfm_exclude_sid")
		cmd = connect_db + f""" "COPY ( 
			select a.sid sid from 
			( select b.gid,array_to_string(array_agg(b.sid),',') as sid from  
				( select gid,sid from mall{mid}.dwd_order_with_category where {rule_cond} sid<> 20442 and sid<> 17745 and sid<> 11661 and     
					to_char(trade_date,'yyyy-mm-dd')>='{begin_date}' and to_char(trade_date,'yyyy-mm-dd')<'{end_date}' {exclude_sid} group by gid,sid 
				) b  group by b.gid 
			) a  
			) TO '{work_dir}order_data_apri_{mid}.csv' " """  # WITH csv header

		returncode, out = common.execute_cmd(cmd)
		if returncode != 0:
			raise SystemExit('execute {0} err :{1}'.format(cmd, out))

		data = []
		with open(work_dir + 'order_data_apri_' + mid + '.csv', encoding='utf8') as f:
			reader = csv.reader(f)  # 加载csv
			# headers = next(reader)  # 第一行，即表头
			# print(headers)
			for row in reader:
				data.append(row)
		# data = [["豆奶", "莴苣"],["莴苣", "尿布", "啤酒", "甜菜"],["豆奶", "尿布", "啤酒", "橙汁"],	["莴苣", "豆奶", "尿布", "啤酒"],
		# ["莴苣", "豆奶", "尿布", "橙汁"]]
		os.remove(work_dir + 'order_data_apri_' + mid + '.csv')

		ctime = strftime('%Y-%m-%d %H:%M:%S', localtime())
		if len(data) == 0:
			logger.info("\t\tmid:{0} 0 条记录".format(mid))
			continue

		result = apyori.apriori(transactions=data, min_support=rule[2], min_confidence=rule[3], min_lift=rule[4], max_length=2)
		"""
		items – 项集，frozenset对象，可迭代取出子集。
		support – 支持度，float类型。
		confidence – 置信度或可信度， float类型。
		ordered_statistics – 存在的关联规则
		可迭代，迭代后，其元素的属性：
		items_base – 关联规则中的分母项集
		confidence – 上面的分母规则所对应的关联规则的可信度
		"""
		logger.info("\t\tmid:{0} 总人数:{1}".format(mid, len(data)))
		lines = []
		for i in result:
			lists_item = list(i.items)
			for j in i.ordered_statistics:
				lists_base = list(j.items_base)
				lists_add = list(j.items_add)
				if lists_base != []:
					line = []
					line.append(mid)
					line.append(rule[0])
					line.append(rule[1])
					line.append(' '.join(lists_item))
					line.append(i.support)
					line.append(' '.join(lists_base))
					line.append(' '.join(lists_add))
					line.append(j.confidence)
					line.append(j.lift)
					line.append(0)
					line.append(0)
					line.append(ctime)
					lines.append(line)

		"""
		k=0
		#按支持度取前max_rule_num条
		lines.sort(key = lambda x:x[4], reverse=True)
		with open(work_dir+'result_data_apri_'+mid+'.csv', 'w') as f:
			s='mid,rule_type,rule_name,rule_set,support,rule_set1,rule_set2,confidence,lift,kulc,ir,ctime\n'
			f.write(s)
			for i in lines:
				s='%s,%d,%s,%s,%.4f,%s,%s,%.4f,%.4f,%d,%d,%s\n' % (i[0],i[1],i[2],i[3],i[4],i[5],i[6],i[7],i[8],i[9],i[10],i[11])
				if k < max_rule_num :
					f.write(s)
					k+=1
				else :
					break
		print("\t\tmid:",mid,"关联规则数:",k)
		"""
		# 按kulc值取数据
		if len(lines) == 0:
			logger.info("\t\tmid:{0} 关联规则数:{1}".format(mid, len(lines)))
			continue
		data = pd.DataFrame(lines)
		data.rename(columns={0: 'mid', 1: 'rule_type', 2: 'rule_name', 3: 'rule_set', 4: 'support', 5: 'rule_set1',
							 6: 'rule_set2', 7: 'confidence', 8: 'lift', 9: 'kulc', 10: 'ir', 11: 'ctime'},
					inplace=True)
		conf1 = 0
		conf2 = 0
		rule_set1 = ''
		rule_set2 = ''
		data = data[data['rule_set1'] > '0']
		data = data[data['rule_set2'] > '0']
		for i in range(0, len(data)):
			if i == 0:
				conf1 = data.iloc[i]['confidence']
				rule_set1 = data.iloc[i]['rule_set']
				continue
			conf2 = data.iloc[i]['confidence']
			rule_set2 = data.iloc[i]['rule_set']
			if rule_set1 == rule_set2:
				data.iloc[i, 9] = (conf1 + conf2) / 2
				data.iloc[i - 1, 9] = (conf1 + conf2) / 2
				data.iloc[i, 10] = conf2 / conf1
				data.iloc[i - 1, 10] = conf1 / conf2
			conf1 = conf2
			rule_set1 = rule_set2
		data = data[data['kulc'] > 0]
		data = data.sort_values(by=["kulc", "rule_set"], ascending=(False))
		data.reset_index(drop=True)
		if len(data) > max_rule_num:
			data = data.iloc[:max_rule_num]
		# data.reset_index(drop = True)
		logger.info("\t\tmid:{0} 关联规则数: {1}".format(mid, len(data)))
		data.to_csv(work_dir + 'result_data_apri_' + mid + '.csv', encoding='utf_8_sig', index=False, sep=",")
		cmd = connect_db + '\"COPY public.dwa_association_rule FROM \'' + work_dir + 'result_data_apri_' + mid + '.csv\' WITH csv header DELIMITER \',\' ENCODING \'utf8\' \"'
		returncode, out = common.execute_cmd(cmd)
		if returncode != 0:
			raise SystemExit('execute {0} err :{1}'.format(cmd, out))

		# python pandas 如何去掉/保留数据集中的重复行？
		data = data.sort_values(by=['mid', 'rule_type', 'rule_set', 'ir'], ascending=(True, True, True, False))
		data = data.drop_duplicates(subset=['mid', 'rule_type', 'rule_set'], keep='first')
		data = data.sort_values(by=['mid', 'rule_type', 'ir'], ascending=(True, True, False))
		if len(data) > 30:
			data = data.iloc[:30]
		if len(data) < 10:
			continue
		try:
			data['width'] = pd.qcut(data['ir'], q=[0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1], precision=2,
								labels=[.1, .2, .25, .3, .35, .4, .45, 0.6, 1.0, 2])
		except:
			continue
		# df_shop['id'] = df_shop['id'].astype('object')
		data['rule_set1'] = data['rule_set1'].astype('int64')
		data['rule_set2'] = data['rule_set2'].astype('int64')
		data = pd.merge(data, df_shop, left_on='rule_set1', right_on='id', how='left')
		data = pd.merge(data, df_shop, left_on='rule_set2', right_on='id', how='left')

		# 绘制网络图https://oomake.com/question/3891541
		net_grid = nx.DiGraph()
		pos = nx.circular_layout(net_grid)
		for i in range(0, len(data)):
			n1 = data.iloc[i]['sname_x']
			n2 = data.iloc[i]['sname_y']
			n3 = data.iloc[i]['width']
			net_grid.add_node(n1)
			net_grid.add_node(n2)
			net_grid.add_edge(n1, n2, width=n3)
		nx.draw_networkx_nodes(net_grid, pos=nx.circular_layout(net_grid), node_color='#ff0000', node_size=10,
							   alpha=0.6)  # 点的样式
		for i in range(0, len(data)):
			n1 = data.iloc[i]['sname_x']
			n2 = data.iloc[i]['sname_y']
			n3 = data.iloc[i]['width']
			nx.draw_networkx_edges(net_grid, pos=nx.circular_layout(net_grid), edgelist=[(n1, n2)], width=n3,
								   edge_color='b')
		nx.draw_networkx_labels(net_grid, pos=nx.circular_layout(net_grid), font_size=6, font_color='k', alpha=0.6)
		plt.title(mid + '.' + rule[1] + "店铺关联关系")
		plt.rcParams['font.family'] = ['sans-serif']
		plt.rcParams['font.sans-serif'] = ['SimSun']
		file_name = work_dir + 'png/apri_' + mid + '_' + str(rule[0]) + '_' + strftime('%Y-%m-%d', localtime()) + '.png'
		plt.savefig(file_name, dpi=200)
		plt.close('all')

		cdn_url = upload_file.upload_to_CDN(file_name)
		if cdn_url == '':
			logger.error("图片上传CDN失败,及时查找原因。")

		conn = psycopg2.connect(host=postgres_host, user=postgres_user, password=postgres_password,
								database=postgres_datebase)
		cur = conn.cursor()
		delete_sql = 'delete from public.dwa_association_rule_graph where to_char(ctime,\'yyyy-mm-dd\')=\'' + strftime(
			'%Y-%m-%d', localtime()) + '\' and mid=' + mid + 'and rule_type=' + str(rule[0])
		cur.execute(delete_sql)
		img_buffer = None
		with open(file_name, mode='rb') as reader:
			img_buffer = reader.read()
		insert_sql = 'INSERT INTO public.dwa_association_rule_graph(mid, rule_type, rule_name, cdn_url, network) ' \
					 'VALUES (' + mid + ',' + str(rule[0]) + ',\'' + rule[1] + '\',\'' + cdn_url + '\',%s)'
		params = (psycopg2.Binary(img_buffer),)
		cur.execute(insert_sql, params)
		conn.commit()
		conn.close()

		os.remove(work_dir + 'result_data_apri_' + mid + '.csv')
		logger.info("\t\tmid:{0} Duration {1}".format(mid, datetime.datetime.now() - start_time))


def test():  # 比较单进程与多进程运行结果的差异
	categ_list = [
		[40000013, '零售', 0.001, 0.005, .01],
		[40000117, '家居', 0.001, 0.005, .01]
	]
	mall_id = ['2']
	for i in mall_id:
		mall_apriori(i)
	logger.info("Total duration of apririo:{}".format(datetime.datetime.now() - start_time))
	sys.exit(0)


if __name__ == '__main__':
	start_time = datetime.datetime.now()
	max_rule_num = 200
	work_dir, connect_db, postgres_host, postgres_port, postgres_user, postgres_password, postgres_datebase \
		= common.get_config()
	mall_id = common.get_mallID()
	# 业态编码、业态名称，最小支持度，最小置信度，最小提升度
	rule_type = [
		[40000013, '零售', 0.001, 0.005, .01],
		[40000117, '家居', 0.001, 0.005, .01],
		[40000224, '娱乐', 0.001, 0.005, .01],
		[40000237, '餐饮', 0.001, 0.005, .01],
		[0, '全场', 0.1, 0.005, .01]]
	logger = common.bigdata_logger('bigdata.log')
	logger.info('apririo starting ...')

	# test()
	# exit()

	po = Pool(5)  # 定义个进程池
	for i in mall_id:
		po.apply_async(mall_apriori, (i,), error_callback=common.error_call_back)
		time.sleep(1)
	po.close()
	po.join()
	logger.info("Total duration of apririo: {}".format(datetime.datetime.now() - start_time))
