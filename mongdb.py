import pymongo
import tkitFile
import os,zipfile
import time;  # 引入time模块
import shutil
"""
mongodb数据备份脚本
根据提示即可

"""

print("设置数据库链接 例如：'mongodb://root:pass@localhost:27017/' 不设置默认")
x=input("数据库信息")
#这里定义mongo数据
if x:
    # client = pymongo.MongoClient('mongodb://root:pass@localhost:27017/')
    client = pymongo.MongoClient(x)
else:
    client = pymongo.MongoClient("localhost", 27017)
# client = pymongo.MongoClient('mongodb://root:pass@localhost:27017/')
# DB = client.gpt2Write
#列出数据名
database_list=client.database_names()

#列出表名
# list_collection=DB.list_collection_names(session=None)

#打包目录为zip文件（未压缩）
def make_zip(source_dir, output_filename):
    """
    打包目录为zip文件（未压缩）
    """
    zipf = zipfile.ZipFile(output_filename, 'w')    
    pre_len = len(os.path.dirname(source_dir))
    for parent, dirnames, filenames in os.walk(source_dir):
        for filename in filenames:
            pathfile = os.path.join(parent, filename)
            arcname = pathfile[pre_len:].strip(os.path.sep)     #相对路径
            zipf.write(pathfile, arcname)
    zipf.close()
# print(DB.name)

def save_collection(collection_name,DB):
    """
    保存一张表
    """
    path=os.path.join("data",DB.name)
    tkitFile.File().mkdir(path)
    json_save=os.path.join("data",DB.name,collection_name+".json")
    json_backup=tkitFile.Json(json_save)
    for it in DB[collection_name].find():
        # print(it)
        try:
            json_backup.save([it])
        except:
            print("error")
            print(it)
            pass

#设置需要备份的数据库
database_list_backup=[]
for db in database_list:
    print("数据库：",db)
    x=input("是否备份(Y or N)")
    if x=="N":
        break
    print("备份：",db)
    database_list_backup.append(db)

# 开始备份
for db in database_list_backup:
    DB=client[db]
    #列出表名
    list_collection=DB.list_collection_names(session=None)
    for it in list_collection:
        # print("数据表：",it)
        # x=input("是否备份(Y or N)")
        # if x=="N":
        #     break
        save_collection(it,DB)
        pass
    #压缩
    path=os.path.join("data",DB.name)
    make_zip(path,"data/"+DB.name+str(int(time.time()))+".zip")
    shutil.rmtree(path)
    print("数据库已经备份：","data/"+DB.name+str(int(time.time()))+".zip")

print("数据库已经备份在目录下：","data/")
