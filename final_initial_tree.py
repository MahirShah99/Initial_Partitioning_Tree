import psycopg2
import math
from math import log2, ceil


# =========================Project===============================
attribute_allocation_dict = {}
colnames = []
tree_dict = {}
partition_list = []
partition_cnt = 1
partition_lst_wh_cnt = []


def getAttributeList(cur, table_name, b):
	"""
		This function returns a dictionary of arrtibutes with attribute fanoutValue 
		of specified table 
	"""
	global attribute_allocation
	global colnames
	try:	
		"""
			Get LIST OF ALL CLOUMNS
		"""
		cur.execute(f"Select * FROM {table_name} LIMIT 1")
		colnames = [desc[0] for desc in cur.description]
		a = len(colnames)
		attribute_allocation = b**(1/a) 

		for i in colnames:
			attribute_allocation_dict[i] = attribute_allocation
		
	except:
		print("error occured")


def least_allocated_alocation():
	"""
		This function returns the attribute having highest attribute allocation
		In case of tie returns the first one
	"""
	temp_list = [(v, u) for u,v in attribute_allocation_dict.items()]
	temp_list = sorted(temp_list, reverse=True)
	return temp_list[0][1]


def getMedian(cur, table_name, attribute, primary_column):
	"""
		This function firsts finds the middle value of the table and then return the value of
		the specified attribute using the middle value
	"""
	cur.execute(f""" select {primary_column} from {table_name}
		where {primary_column}=(select count(*) from {table_name})/2 """)

	ans = cur.fetchall()[0][0]
	cur.execute(f"""select {attribute} from {table_name} where {primary_column}='{ans}' """)
	ans = cur.fetchall()[0][0]

	return ans


def treeDictAdd(i, attribute_name, median_value):
	"""
		This function adds the attribute_name and its partitioning value to the tree node
	"""
	global tree_dict
	tmp = {}
	tmp["attribute_name"] = attribute_name
	tmp["attribute_value"] = median_value

	tree_dict[i] = tmp


def getString(child, parent):
    if( (parent==0) and (child%2 == 0)):
        return (tree_dict[parent]["attribute_name"] + " > " + str(tree_dict[parent]["attribute_value"] ))

    if( (parent==0) and (child%2 != 0)):
        return (tree_dict[parent]["attribute_name"] + " <= " + str(tree_dict[parent]["attribute_value"] ))

    else:
        if(child%2 == 0):
            return (tree_dict[parent]["attribute_name"] + " > " + str(tree_dict[parent]["attribute_value"]) +
            " and " + getString(parent, int(ceil(parent/2)-1) ) )
        else:
            return (tree_dict[parent]["attribute_name"] + " <= " + str(tree_dict[parent]["attribute_value"]) +
            " and " + getString(parent, int(ceil(parent/2)-1) ) )


# main execution
if __name__ == "__main__":
	print("Enter the table number:- ")
	print("1 for part")
	print("2 for supplier")
	print("3 for partsupp")
	print("4 for customer")
	print("5 for nation")
	print("6 for lineitem")
	print("7 for region")
	print("8 for orders")

	table_num = int(input("Enter your Choice:- "))
	num_partition = int(input("Enter number of partition blocks(power of 2):- "))

	if(table_num == 1):
		table_name = "part"
		primary_column = "p_partkey"
	if(table_num == 2):
		table_name = "supplier"
		primary_column = "s_suppkey"
	if(table_num == 3):
		table_name = "partsupp_1"
		primary_column = "ps_primarykey"
	if(table_num == 4):
		table_name = "customer"
		primary_column = "c_custkey"
	if(table_num == 5):
		table_name = "nation"
		primary_column = "n_nationkey"
	if(table_num == 6):
		table_name = "lineitem_1"
		primary_column = "l_primarykey"
	if(table_num == 7):
		table_name = "region"
		primary_column = "r_regionkey"
	if(table_num == 8):
		table_name = "orders_1"
		primary_column = "o_primarykey"

	cnt = 0

	max_depth = int(math.log2(num_partition))
	# print("max depth: ", max_depth)

	conn = psycopg2.connect(database = "postgres", user = "postgres",
	 password = "admin", host = "127.0.0.1", port = "5432")
	print("Opened database successfully")
	cur = conn.cursor()

	"""
		create dictionary with atttribute name as key ad attribute allocation as value
	"""
	getAttributeList(cur, table_name, num_partition)

	for depth in range(max_depth):
		for i in range(pow(2, depth)):
			attribute_name = least_allocated_alocation()
			median_value = getMedian(cur, table_name, attribute_name, primary_column)
			treeDictAdd(cnt, attribute_name, median_value)
			attribute_allocation_dict[attribute_name] -= 2/pow(2, max_depth-depth)
			cnt = cnt+1
	# print("Tree: ",tree_dict)

	for i in range(num_partition-1, 2*num_partition-1):
		partition_list.append(getString(i, int(ceil(i/2)-1) ) )	

	# print(partition_list)

	"""
		Write Partitions into file
	"""
	file = open(table_name+"_partition.txt", "w")
	file.write(str(partition_list))
	file.close()

	part_cnt = 1
	file = open(table_name+"_partition2.txt", "w")
	for i in partition_list:
		file.write("P"+str(part_cnt)+": "+str(partition_list[part_cnt-1])+"\n")
		print("P"+str(part_cnt)+": "+str(partition_list[part_cnt-1]))
		print("="*200)
		part_cnt += 1
	file.close()

	for i in range(1, len(partition_list)+1):
		tmp_table = table_name+"_p_"+str(i)
		try:
			cur.execute(f"drop table if exists {tmp_table}")
		except:
			print("no table")

		temp_lst = partition_list[i-1].split("and")
		condition = ""
		for i in temp_lst:
			p = i.split()
			condition = condition + f"{p[0]} {p[1]} '{p[2]}' and "
		condition = condition[:-4]
		condition = condition.strip()

		msg = "create table "+tmp_table+" as select * from "+table_name+" where "+condition
		try:
			cur.execute(f"create table {tmp_table} as select * from {table_name} where {condition}")
			print("Table Query: ",msg)
			print("table created")
			print("="*200)
			conn.commit()
		except:
			print("Table Query: ",msg)
			print("error")
			print("="*200)
	conn.close()

