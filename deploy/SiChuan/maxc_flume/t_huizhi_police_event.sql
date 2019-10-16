CREATE EXTERNAL TABLE IF NOT EXISTS wzShareProject.t_huizhi_police_event
(
begintime BIGINT COMMENT "事件时间",
event STRING COMMENT "事件类型",
id STRING COMMENT "疑似受害人号码",
homearea STRING COMMENT "受害人号码归属地",
relate_id STRING COMMENT "对端号码",
relate_homearea STRING COMMENT "对端号码归属地",
eventarea STRING COMMENT "事件发生地",
latitude STRING COMMENT "经度",
longitude STRING COMMENT "维度",
operator STRING COMMENT "电信运营商标识",
sp_duration STRING COMMENT "通话时长",
message STRING COMMENT "疑似受害目标接受到的短信内容",
city_code STRING COMMENT "中标受害人目标所在城市编号",
proince_code STRING COMMENT "中标受害人目标所在省份编号",
mode_code STRING COMMENT "模型编码"
)
PARTITIONED BY (uploadDate STRING) 
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\001'
STORED AS TEXTFILE;

ALTER TABLE wzShareProject.t_huizhi_police_event ADD PARTITION(uploadDate=${yyyyMMdd}) LOCATION '/mx_projects/wzShareProject/data/t_huizhi_police_event/${yyyyMMdd}