from pydantic import BaseModel
import json
from datetime import date
from typing import Optional

class Nature_Of_Work(BaseModel):
    work_id:int
    work_name:str
    work_status:int

class User_table(BaseModel):
    user_id:int
    username:str
    password:str
    role:str
    firstname:str
    lastname:str
    location:str
    user_status:int

class tds(BaseModel):
    tds_id:int
    tds:str
    tds_status:int

class gst(BaseModel):
    gst_id:int
    gst:str
    gst_status:int