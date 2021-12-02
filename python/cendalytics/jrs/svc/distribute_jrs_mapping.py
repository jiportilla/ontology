#!/usr/bin/env python
# -*- coding: UTF-8 -*-


import os
import pandas as pd
from collections import Counter
from datamongo import BaseMongoClient
from datamongo import CendantXdm
"""
+----+------------------------+-----------------------------------------------------------------------+-------------+------------------------------------+--------------+-------------------------------------------------+
|    | CendantSkill           | JRSS                                                                  |   JobRoleId | JobRoleName                        | SkillSetId   | SkillSetName                                    |
|----+------------------------+-----------------------------------------------------------------------+-------------+------------------------------------+--------------+-------------------------------------------------|
|  0 | Database Administrator | Application Database Administrator-Physical Security Services         |      040685 | Application Database Administrator | S1866        | Physical Security Services                      |
|  1 | Application Developer  | Application Developer-Brokerage Svcs Integration, Adapters & Prod Ext |      040684 | Application Developer              | S9314        | Brokerage Svcs Integration, Adapters & Prod Ext |
|  2 | Application Developer  | Application Developer-GTS Analytics                                   |      040684 | Application Developer              | S7687        | GTS Analytics                                   |
|  3 | Application Developer  | Application Developer-GTS Labs-Automation & Cognitive Delivery        |      040684 | Application Developer              | S7387        | GTS Labs-Automation & Cognitive Delivery        |
|  4 | Cognitive Skill        | Application Developer-GTS Labs-Automation & Cognitive Delivery        |      040684 | Application Developer              | S7387        | GTS Labs-Automation & Cognitive Delivery        |
|  5 | Application Developer  | Application Developer-GTS Labs-Brokerage Product Development          |      040684 | Application Developer              | S9313        | GTS Labs-Brokerage Product Development          |
+----+------------------------+-----------------------------------------------------------------------+-------------+------------------------------------+--------------+-------------------------------------------------+
"""

if __name__ == "__main__":
    path = os.path.join(os.environ["GTS_BASE"], "resources/output/jrs_mapping.csv")
    df = pd.read_csv(path, sep='\t', encoding='utf-8')


    for job_role_id in df['JobRoleId'].unique():
        df2 = df[df['JobRoleId'] == job_role_id]

        print (df2['JobRoleName'].unique())
        cendant_skills = df2['CendantSkill'].unique()
        print (cendant_skills)
        print ("\n")




