# Reading an excel file using Python
import xlrd

# Give the location of the file
loc = ("/Users/thanh/Downloads/DS_App.xls")

# To open Workbook
wb = xlrd.open_workbook(loc)
sheet = wb.sheet_by_index(2)

# For row 0 and column 0
#print(sheet.cell_value(0, 0))

# For row 0 and column 0
## sheet.cell_value(0, 0)

## print(" CREATE TABLE beth.project ")
## print(" ( ")
## print("  id SERIAL PRIMARY KEY, ")

## for i in range(sheet.ncols):

##   print(sheet.cell_value(0, i).lower().replace(" ", "_") + " VARCHAR (500), ")

## print(" ); ")

for i in range(sheet.nrows):
    print( "INSERT INTO project ("
           "stage_title, project_name, employer, client_industry, "
           "nature_of_project, client_name, primary_role, start_dt, end_dt, start_involvement_dt, "
           "end_involvement_dt, business_need, bu_org_name, country_name, project_level) "
           "VALUES ( '" +
           sheet.cell_value(i, 0).replace("'","") + "','" +
           sheet.cell_value(i, 1).replace("'","") + "','" +
           sheet.cell_value(i, 2).replace("'","") + "','" +
           sheet.cell_value(i, 3).replace("'","") + "','" +
           sheet.cell_value(i, 4).replace("'","") + "','" +
           sheet.cell_value(i, 5).replace("'","") + "','" +
           sheet.cell_value(i, 6).replace("'","") + "','" +
           sheet.cell_value(i, 7).replace("'","") + "','" +
           sheet.cell_value(i, 8).replace("'","") + "','" +
           sheet.cell_value(i, 9).replace("'","") + "','" +
           sheet.cell_value(i, 10).replace("'","") + "','" +
           sheet.cell_value(i, 35).replace("'","").replace("&","-").replace('\n',"").replace('\r',"").replace("  "," ")  + "','" +
           sheet.cell_value(i, 72).replace("'","") + "','" +
           sheet.cell_value(i, 73).replace("'","") +
           "'," + "3 );")



