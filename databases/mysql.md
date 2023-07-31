# mysql
## 建表
建表的基本结构
```sql
create table my_table(
    id INT(50) PRIMIARY KEY AUTO_INCREMENT, -- 主键/自增
    name varchar(50) NOT NULL,              -- 是否可以为空
    age INT(50) DEFAULT 18,                 -- 默认值
    gender INT(10) COMMENT '性别'           -- 注释信息
);
```
### 自增列
在mysql建表过程中设定某列为自增列，它可以**自动递归地生成唯一数值**。  
通常情况下，自增列是作为主键进行使用的；功能可以唯一地标识数据中的一条记录。  
自增列的好处：
1. 作为主键保证数据库每一个记录都有一个唯一标识；
2. 方便表的维护和管理（在表结构要改变或者要批量修改数据时，可以使用where id> id< 而不用各种判断等于；或者在数据完全相同的两个数据要插入表时，自增列可以使其不报错）；
3. 提高数据库的性能，作为索引使用可以加快查询和排序速度。

插入数据时，无需指定自增列的数值，其会自动增加。  

## 表处理
### mysql-connector
hdfs或csv文件要写入MySQL表中时，可以使用mysql-connector库函数进行处理。  
```python
import mysql.connector
db = mysql.connector.connect(user='', password='', host='', port='', database='')
cursor = db.cursor()
query = f"insert into xxx"
cursor.executemany(query, values)
db.commit()
db.close()
```