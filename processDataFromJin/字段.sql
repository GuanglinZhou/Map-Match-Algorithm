CREATE TABLE "EASYITS"."TRACK_TAXIMETER" 

   (	"ID" NUMBER(19,0), 

	"COMPANY_ID" NUMBER(19,0), 

	"CAR_NO" VARCHAR2(19 BYTE), 

	"START_TIME" DATE, 

	"END_TIME" DATE, 

	"STATUS" NUMBER(5,0), 

	"TRAVEL_TIME" NUMBER(19,0) DEFAULT 0, 

	"START_LT" NUMBER(13,10) DEFAULT 0, 

	"END_LT" NUMBER(13,10) DEFAULT 0, 

	"START_LG" NUMBER(13,10) DEFAULT 0, 

	"END_LG" NUMBER(13,10) DEFAULT 0, 

	"MILEAGE" NUMBER(13,2) DEFAULT 0, 

	"COMPANY_NAME" VARCHAR2(50 BYTE), 

	"START_ADDRESS" VARCHAR2(150 BYTE), 

	"START_ADDRESS_DESC" VARCHAR2(200 BYTE), 

	"END_ADDRESS" VARCHAR2(150 BYTE), 

	"END_ADDRESS_DESC" VARCHAR2(200 BYTE)

   )


   31684192,
   341,
   'A82127',
   to_date('07-1月 -17','DD-MON-RR'),
   to_date('07-1月 -17','DD-MON-RR'),
   1,
   278000,（单位：毫秒）
   31.8959833333,
   31.88555,
   117.2676166667,
   117.26335,
   2222.08,（单位：米）
   '合肥市新亚出租汽车有限公司',
   '安徽省合肥市庐阳区藕塘路',
   '金荷苑北117米',
   '安徽省合肥市庐阳区义井路1号',
   '合肥市南国花园小学南62米'



    (31684216,
   	341,
   	'A83318',
   	to_date('07-1月 -17','DD-MON-RR'),
   	to_date('07-1月 -17','DD-MON-RR'),
   	0,
   	4912000,
   	31.7842833333,
   	31.86325,
   	117.2220166667,
   	117.2450666667,
   	18695.14,
   	'合肥市新亚出租汽车有限公司',
   	'安徽省合肥市蜀山区金寨南路',
   	'安徽宏实汽车销售有限公司（江铃福特经销商）南119米',
   	'安徽省合肥市蜀山区合作化北路168号',
   	'合肥西苑中学东南51米');