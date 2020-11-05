package com.atguigu.gmall.realtime.bean

/*
@author zilong-pan
@creat 2020-10-28 10:49
@desc $
*/ case class User_info(
                         id:String,
                         user_level:String,
                         birthday:String,
                         gender:String,
                         var age_group:String,//年龄段
                         var gender_name:String
                       )
