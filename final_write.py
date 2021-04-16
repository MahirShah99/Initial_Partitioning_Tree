import psycopg2
from math import log2
from datetime import datetime

print("Enter Table Number:- ")
print("1 for part")
print("2 for supplier")
print("3 for partsupp")
print("4 for customer")
print("5 for nation")
print("6 for lineitem")
print("7 for region")
print("8 for orders")
table_num = int(input("Enter your Choice:- "))
print()

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

partition_l = []

try:
    file = open(table_name+"_partition.txt")
    data = file.read()
    data = data.strip("[]").split(", ")

    for i in data:
        partition_l.append(i.strip("'"))

    no_of_bucket = len(partition_l)

    max_depth = int(log2(no_of_bucket))

    d = {}
    partition_path_cnt = []

    if(table_num==1):
        d["p_name"] = input("Enter Name:- ")                 
        d["p_mfgr"] = input("Enter MFGR:- ")
        d["p_brand"] = input("Enter Brand:- ")
        d["p_type"] = input("Enter Type:- ")
        d["p_size"] = int(input("Enter Size (Numeric):- "))               
        d["p_container"] = input("Enter Container:- ")
        d["p_retailprice"] = int(input("Enter RetailPrice (Numeric):- "))  
        d["p_comment"]  = input("Ener Comment:- ")
    if(table_num==2):    
        d["s_name"] = input("enter name:- ")
        d["s_address"] = input("enter address:- ")
        d["s_nationkey"] = int(input("enter nationkey (numeric):- "))
        d["s_phone"] = input("enter phone (eg:- 12-123-123-1234):- ")
        d["s_acctbal"] = int(input("enter balance (numeric):- "))
        d["s_comment"] = input("enter comment:- ")
    if(table_num==3):
        d["ps_partkey"] = int(input("Enter PartKey (Numeric):- "))
        d["ps_suppkey"] = int(input("Enter SuppKey (Numeric):- "))
        d["ps_availqty"] = int(input("Enter Quantity (Numeric):- "))
        d["ps_supplycost"] = int(input("Enter SupplyCost (Numeric):-  "))
        d["ps_comment"] = input("Enter Comment:- ")
    if(table_num==4):
        d["c_name"] = input("Enter name:- ")
        d["c_address"] = input("Enter Address:- ")
        d["c_nationkey"] = int(input("Enter nationKey (Numeric):- "))
        d["c_phone"] = input("Enter Phone (EG: 25-989-741-2988):- ")
        d["c_acctbal"] = int(input("Enter Balance(Numeric):- "))
        d["c_mktsegment"] = input("Enter Segment:- ")
        d["c_comment"] = input("Enter Comment:- ")
    if(table_num==5):
        d["n_name"] = input("Enter Name:- ")
        d["n_regionkey"] = int(input("Enter regionKey(Numeric):- "))
        d["n_comment"] = input("Enter Comment:- ")
    if(table_num==6):
        d["l_orderkey"] = int(input("Enter Orderkey(Numeric):- "))
        d["l_partkey"] = int(input("Enter PartKey(Numeric):- "))
        d["l_suppkey"] = int(input("Enter SuppKey(Numeric):- ")) 
        d["l_linenumber"] = int(input("Enter LineNumber(Numeric):- "))
        d["l_quantity"] = int(input("Enter Quantity(Numeric):- "))
        d["l_extendedprice"] = int(input("Enter ExtendedPrice(Numeric):-  "))
        d["l_discount"] = int(input("Enter Discount(Numeric):-  "))
        d["l_tax"] = int(input("Enter Tax(Numeric):-  "))
        d["l_returnflag"] = input("Enter Return Flag:- ")
        d["l_linestatus"] = input("Enter LineStatus:- ")
        d["l_shipdate"] = input("Enter ShipDate(EG: YYYY-MM-DD):- ")
        d["l_shipdate"] = datetime.strptime(d["l_shipdate"], "%Y-%m-%d").date()
        d["l_commitdate"] = input("Enter CommitDate(EG: YYYY-MM-DD):- ")
        d["l_commitdate"] = datetime.strptime(d["l_commitdate"], "%Y-%m-%d").date()
        d["l_receiptdate"] = input("Enter ReceiptDate(EG: YYYY-MM-DD):- ")
        d["l_receipttdate"] = datetime.strptime(d["l_receiptdate"], "%Y-%m-%d").date()
        d["l_shipinstruct"] = input("Enter Instruction:- ")
        d["l_shipmode"] = input("Enter Mode:- ")
        d["l_comment"] = input("Emter Comment:- ")
    if(table_num==7):
        d["r_name"] = input("Enter Region Name:- ")
        d["r_comment"] = input("Enter Comment:- ")
    if(table_num==8):
        d["o_orderkey"] = int(input("Enter OrderKey(Numeric):- "))
        d["o_custkey"] = int(input("Enter CustKey(Numeric):- "))
        d["o_orderstatus"] = input("Enter OrderStatus:- ")
        d["o_totalprice"] = int(input("Enter TotalPrice(Numeric):- "))
        d["o_orderdate"] = input("Enter OrderDate(EG: YYYY-MM-DD):- ")
        d["o_orderdate"] = datetime.strptime(d["o_orderdate"], "%Y-%m-%d").date()
        d["o_orderpriority"] = input("Enter Order Priority:- ")
        d["o_clerk"] = input("Enter Clerk:- ")
        d["o_shippriority"] = int(input("Enter ShipPriority(Numeric):- "))
        d["o_comment"] = input("Enter Comment:- ")
    
    print()
    conn = psycopg2.connect(database = "postgres", user = "postgres",
     password = "admin", host = "127.0.0.1", port = "5432")
    print("Opened database successfully")
    print()
    cur = conn.cursor()

    cur.execute(f"select {primary_column} from {table_name} order by {primary_column} desc limit 1")
    d[primary_column] = cur.fetchall()[0][0] + 1

    input_l = [(u,v) for u,v in d.items()]    
    flag=1
    for i in partition_l:
        cnt = 0
        flag = 1
        for j in i.split("and"):
            k = j.split()
            if(k[1] == "<="):
                if (isinstance(d[k[0]], int)):
                    if("." in k[2]):
                        k[2] = k[2].split(".")[0]
                    if(d[k[0]] <= int(k[2])):
                        cnt += 1
                else:
                    if(d[k[0]] <= k[2]):
                        cnt += 1
            if(k[1] == ">"):
                if (isinstance(d[k[0]], int)):
                    if("." in k[2]):
                        k[2] = k[2].split(".")[0]
                    if(d[k[0]] > int(k[2])):
                      cnt += 1
                else:
                    if(d[k[0]] > k[2]):
                        cnt += 1

        if(cnt == max_depth):
            partition = partition_l.index(i)+1
            # print("New row inserted in partition: ",partition)
            flag = 0
            break
        else:
            partition_path_cnt.append((cnt, i))

    if(flag):
        # print(partition_path_cnt)    
        partition_path_cnt = sorted(partition_path_cnt, reverse=True)
        # print(partition_path_cnt)
        paritition_tmp = partition_path_cnt[0][1]
        paritition_tmp = paritition_tmp.strip()
        partition = partition_l.index(paritition_tmp)+1


    if(table_num==1):
        try:
            cur.execute(f""" insert into part(p_name, p_mfgr, p_brand, p_type, p_size, p_container, 
                p_retailprice, p_comment) values('{d['p_name']}','{d['p_mfgr']}','{d['p_brand']}',
                '{d['p_type']}','{d['p_size']}','{d['p_container']}','{d['p_retailprice']}','{d['p_comment']}') """)
            conn.commit()
            # print("insert success")
            cur.execute("select p_partkey from part order by p_partkey desc limit 1")
            p_key = cur.fetchall()[0][0]
            cur.execute(f""" insert into part_p_{partition}(p_partkey,p_name, p_mfgr, p_brand, p_type, p_size, p_container, 
                p_retailprice, p_comment) values({p_key},'{d['p_name']}','{d['p_mfgr']}','{d['p_brand']}',
                '{d['p_type']}','{d['p_size']}','{d['p_container']}','{d['p_retailprice']}','{d['p_comment']}') """)
            conn.commit()
            print("New row inserted in partition: ",partition)
            print("insert successfull")
        except Exception as e:
            print(e)
    if(table_num==2):
        try:
            cur.execute(f""" insert into {table_name}(s_name, s_address, s_nationkey, s_phone,
                s_acctbal, s_comment) values('{d['s_name']}', '{d['s_address']}', '{d['s_nationkey']}', 
                '{d['s_phone']}', '{d['s_acctbal']}', '{d['s_comment']}') """)
            conn.commit()
            # print("insert success")
            cur.execute(f"select {primary_column} from {table_name} order by {primary_column} desc limit 1")
            s_key = cur.fetchall()[0][0]
            cur.execute(f""" insert into {table_name}_p_{partition}(s_suppkey, s_name, s_address, s_nationkey, s_phone,
                s_acctbal, s_comment) values('{s_key}','{d['s_name']}', '{d['s_address']}', '{d['s_nationkey']}', 
                '{d['s_phone']}', '{d['s_acctbal']}', '{d['s_comment']}') """)
            conn.commit()
            print("New row inserted in partition: ",partition)
            print("insert successfull")
        except Exception as e:
            print(e)
    if(table_num==3):
        try:
            cur.execute(f""" insert into {table_name}(ps_partkey, ps_suppkey,
             ps_availqty,ps_supplycost,ps_comment) values('{d['ps_partkey']}','{d['ps_suppkey']}',
             '{d['ps_availqty']}','{d['ps_supplycost']}','{d['ps_comment']}') """)
            conn.commit()
            # print("insert success")
            cur.execute(f"select {primary_column} from {table_name} order by {primary_column} desc limit 1")
            ps_key = cur.fetchall()[0][0]        
            cur.execute(f""" insert into {table_name}_p_{partition}(ps_primarykey,ps_partkey, ps_suppkey,
             ps_availqty,ps_supplycost,ps_comment) values('{ps_key}','{d['ps_partkey']}','{d['ps_suppkey']}',
             '{d['ps_availqty']}','{d['ps_supplycost']}','{d['ps_comment']}') """)
            conn.commit()
            print("New row inserted in partition: ",partition)
            print("insert successfull")
        except Exception as e:
            print(e)
    if(table_num==4):    
        try:
            cur.execute(f""" insert into {table_name}(c_name,c_address,c_nationkey,
            c_phone,c_acctbal,c_mktsegment,c_comment) values('{d['c_name']}','{d['c_address']}',
             '{d['c_nationkey']}','{d['c_phone']}','{d['c_acctbal']}','{d['c_mktsegment']}',
             '{d['c_comment']}') """)
            conn.commit()
            # print("insert success")
            cur.execute(f"select {primary_column} from {table_name} order by {primary_column} desc limit 1")
            c_key = cur.fetchall()[0][0]        
            cur.execute(f""" insert into {table_name}_p_{partition}(c_custkey,c_name,c_address,c_nationkey,
            c_phone,c_acctbal,c_mktsegment,c_comment) values('{c_key}','{d['c_name']}','{d['c_address']}',
             '{d['c_nationkey']}','{d['c_phone']}','{d['c_acctbal']}','{d['c_mktsegment']}',
             '{d['c_comment']}') """)
            conn.commit()
            print("New row inserted in partition: ",partition)
            print("insert successfull")
        except Exception as e:
            print(e)
    if(table_num==5):
        try:
            cur.execute(f""" insert into {table_name}(N_NAME,N_REGIONKEY,N_COMMENT)
             values('{d['n_name']}','{d['n_regionkey']}','{d['n_comment']}') """)
            conn.commit()
            # print("insert successful")
            cur.execute(f"select {primary_column} from {table_name} order by {primary_column} desc limit 1")
            n_key = cur.fetchall()[0][0]    
            cur.execute(f""" insert into {table_name}_p_{partition}(n_nationkey,N_NAME,N_REGIONKEY,N_COMMENT)
             values('{n_key}','{d['n_name']}','{d['n_regionkey']}','{d['n_comment']}') """)
            conn.commit()
            print("New row inserted in partition: ",partition)
            print("insert successfull")
        except Exception as e:
            print(e)
    if(table_num==6):
        try:
            cur.execute(f""" insert into {table_name}(L_ORDERKEY, L_PARTKEY,L_SUPPKEY,
                L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,
                 L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,
                 L_COMMENT) values ('{d['l_orderkey']}','{d['l_partkey']}','{d['l_suppkey']}',
                 '{d['l_linenumber']}','{d['l_quantity']}','{d['l_extendedprice']}',
                 '{d['l_discount']}','{d['l_tax']}','{d['l_returnflag']}',
                 '{d['l_linestatus']}','{d['l_shipdate']}','{d['l_commitdate']}','{d['l_receiptdate']}',
                 '{d['l_shipinstruct']}','{d['l_shipmode']}','{d['l_comment']}')""")   
            conn.commit()
            # print("insert success")
            cur.execute(f"""select {primary_column} from {table_name} order by {primary_column} desc limit 1""")
            l_key = cur.fetchall()[0][0]
            cur.execute(f""" insert into {table_name}_p_{partition}(l_primarykey,L_ORDERKEY, L_PARTKEY,L_SUPPKEY,
                L_LINENUMBER,L_QUANTITY,L_EXTENDEDPRICE,L_DISCOUNT,L_TAX,L_RETURNFLAG,
                 L_LINESTATUS,L_SHIPDATE,L_COMMITDATE,L_RECEIPTDATE,L_SHIPINSTRUCT,L_SHIPMODE,
                 L_COMMENT) values ('{l_key}','{d['l_orderkey']}','{d['l_partkey']}','{d['l_suppkey']}',
                 '{d['l_linenumber']}','{d['l_quantity']}','{d['l_extendedprice']}',
                 '{d['l_discount']}','{d['l_tax']}','{d['l_returnflag']}',
                 '{d['l_linestatus']}','{d['l_shipdate']}','{d['l_commitdate']}','{d['l_receiptdate']}',
                 '{d['l_shipinstruct']}','{d['l_shipmode']}','{d['l_comment']}')""")   
            conn.commit()
            print("New row inserted in partition: ",partition)
            print("insert successfull")
        except Exception as e:
            print(e)
    if(table_num==7):
        try:
            cur.execute(f"insert into region(r_name, r_comment) values('{d['r_name']}', '{d['r_comment']}') ")
            conn.commit()
            # print("insert success")
            cur.execute("select r_regionkey from region order by r_regionkey desc limit 1")
            r_key = cur.fetchall()[0][0]
            cur.execute(f"""insert into region_p_{partition}(r_regionkey,r_name, r_comment)
             values('{r_key}','{d['r_name']}', '{d['r_comment']}') """)
            conn.commit()
            print("New row inserted into partition: ",partition)
            print("insert successfull")
        except Exception as e:
            print(e)
    if(table_num==8):
        try:
            cur.execute(f""" insert into {table_name}(O_ORDERKEY, O_CUSTKEY,O_ORDERSTATUS,
                O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY, O_COMMENT )
                 values('{d['o_orderkey']}','{d['o_custkey']}','{d['o_orderstatus']}',
                 '{d['o_totalprice']}','{d['o_orderdate']}','{d['o_orderpriority']}',
                 '{d['o_clerk']}','{d['o_shippriority']}','{d['o_comment']}') """)  
            conn.commit()
            # print("insert success")

            cur.execute(f"""select {primary_column} from {table_name} order by {primary_column} desc limit 1 """)
            o_key = cur.fetchall()[0][0]

            cur.execute(f""" insert into {table_name}_p_{partition}(o_primarykey,O_ORDERKEY, O_CUSTKEY,O_ORDERSTATUS,
                O_TOTALPRICE,O_ORDERDATE,O_ORDERPRIORITY,O_CLERK,O_SHIPPRIORITY, O_COMMENT )
                 values('{o_key}','{d['o_orderkey']}','{d['o_custkey']}','{d['o_orderstatus']}',
                 '{d['o_totalprice']}','{d['o_orderdate']}','{d['o_orderpriority']}',
                 '{d['o_clerk']}','{d['o_shippriority']}','{d['o_comment']})' """)  
            conn.commit()
            print("New row inserted in partition: ",partition)
            print("insert successfull")

        except Exception as e:
            print(e)        
except Exception as e:
    print(e)

finally:
    conn.close()