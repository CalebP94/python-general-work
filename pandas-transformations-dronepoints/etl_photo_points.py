import pyodbc
import arcpy
from arcpy.management import GeoTaggedPhotosToPoints
import os
import pandas as pd
import sqlalchemy 

class ETL_Processing():
        #establish class attributes
        server = os.environ['SERVERCRED']
        database = os.environ['DATABASENAME']
        user_name=['USER']
        driver = '{ODBC Driver 17 for SQL Server}'
        connection_string = f"DRIVER={self.driver};SERVER={self.server};DATABASE={self.database};Trusted_Connection=yes"
        conn = pyodbc.connect(self.connection_string)
        gdb_base_dronepoints = os.environ['gdb_base_path_dronepoints']
        gdb_base_dronpointstable = os.environ['gdb_base_path_dronepointstable']
        dron_survey_dir_base= os.environ['dronephotopoints_base_dir']
    def __init__(self) -> None:

        self.circuitID_list = []
        self.state_list = []
        self.area_list = []
        self.ops_ctr = []
        self.aprx = 'C:\\Users\\CPenni3\\OneDrive - Duke Energy\\Documents\\ArcGIS\\Projects\\GeoTagged_PhotoPoints\\GeoTagged_PhotoPoints.aprx'
        
    def queryJira(self):
        cursor = self.conn.cursor()
        query = """with CTE as
                (SELECT LEFT(JIssue.SUMMARY,8) AS CircuitID, MAX(CG.CREATED) AS Max_Date, CAST(CI.NEWSTRING as nvarchar(50)) 
                AS Last_Status, Jissue.issuestatus FROM [JiraDC_PROD].[jiraschema].jiraissue JIssue JOIN [JiraDC_PROD].[jiraschema].project PRJ 
                ON JIssue.PROJECT = PRJ.id JOIN [JiraDC_PROD].jiraschema.changegroup CG ON JIssue.ID = CG.issueid JOIN [JiraDC_PROD].[jiraschema].changeitem CI 
                ON CG.ID = CI.groupid AND CI.FIELD = 'status' and FIELDTYPE = 'jira' WHERE PRJ.ID = 33113 AND (issuestatus IN ('28316', '34817','34818', '34819', '34820','34821', '37201')) 
                GROUP BY LEFT(JIssue.SUMMARY,8), 
                CAST(CI.NEWSTRING as nvarchar(50)), Jissue.issuestatus)

                SELECT CircuitID, Count(CircuitID) as CircuitID from CTE 
                Group By CircuitID"""
        
        print("Connected sucessfully")
        cursor.execute(query)
        rows = list(cursor.fetchall())

        catch_list = []
        for row in rows:
            #print(row[0], row[1])
            arr=[row[0], row[1]]
            catch_list.append(arr)
        cursor.close()
        self.conn.close()
        return catch_list
    
    def compare_jira_xlsx(self):
        jira = self.queryJira()
        df_jira = pd.DataFrame(jira, columns=["circuitid","count"])
        df_xlsx_photoPoints = pd.read_excel('C:\\Users\\Cpenni3\\OneDrive - Duke Energy\\CDGI\PowerBI\\Drone_Photo_Point_Circuits.xlsx')
        df_xlsx_masterCirc = pd.read_excel('C:\\Users\\Cpenni3\\OneDrive - Duke Energy\\CDGI\\PowerBI\\Circuit_Master.xlsx')
        merged_photoPoints = pd.merge(df_jira, df_xlsx_photoPoints, on="circuitid", how="left")
        circ_nonMatch = merged_photoPoints[merged_photoPoints['OBJECTID'].isna()]
        circ_nonMatch = pd.DataFrame(circ_nonMatch['circuitid'])
        circ_nonMatch['circuitid'] = circ_nonMatch['circuitid'].astype(str)
        df_xlsx_masterCirc['circuitid'] = df_xlsx_masterCirc['circuitid'].astype(str)
        merge_masterCirc = pd.merge(circ_nonMatch, df_xlsx_masterCirc, on="circuitid", how="left")
        #merge_masterCirc[['Substation Metric Op Center', 'Substation Metric Op CenterII']] = merge_masterCirc['Substation Metric Op Center'].str.split(' ', 0, expand=True)
        self.circuitID_list = merge_masterCirc['circuitid'].tolist()
        self.state_list = merge_masterCirc['Substation State'].tolist()
        self.area_list = merge_masterCirc['Substation Zone'].tolist()
        self.ops_ctr = merge_masterCirc['Substation Metric Op Center'].tolist()


    def run_geotagged(self):
        self.compare_jira_xlsx()
        for i, row in enumerate(self.circuitID_list):
            base = self.directory_base.format(self.state_list[i], self.area_list[i], self.ops_ctr[i], self.circuitID_list[i])
            outPutFeatureClass = self.gdb_base.format(self.circuitID_list[i])
            outPutFeatureTable = self.gdb_base_tb.format(self.circuitID_list[i])
            if not arcpy.Exists(outPutFeatureClass):
                arcpy.AddMessage("Circuit geo tagged photos not created in GIS")
                base = self.directory_base.format(self.state_list[i], self.area_list[i], self.ops_ctr[i], self.circuitID_list[i])
                outPutFeatureClass = self.gdb_base.format(self.circuitID_list[i])
                outPutFeatureTable = self.gdb_base_tb.format(self.circuitID_list[i])
                arcpy.AddMessage("RUNNING: {} and {}".format(outPutFeatureClass, outPutFeatureTable))
                try:
                    arcpy.env.overwriteOutput = True
                    GeoTaggedPhotosToPoints(base, outPutFeatureClass, outPutFeatureTable, Include_Non_GeoTagged_Photos='ONLY_GEOTAGGED', Add_Photos_As_Attachments='NO_ATTACHMENTS')
                    self.append_to_photoPoints(outPutFeatureClass)
                except Exception as e:
                    arcpy.AddMessage(e)
                    continue
            else:
                arcpy.AddMessage("Already Exist")
                continue

    def append_to_photoPoints(self, outPutFeatureClass):
        aprx = arcpy.mp.ArcGISProject(self.aprx)
        map_obj = aprx.listMaps("Map")[0]
        target_layer = map_obj.listLayers("Drone_Photo_Point")[0]

        fieldMappings = arcpy.FieldMappings()
        field_map_name= arcpy.FieldMap()
        field_map_path = arcpy.FieldMap()

        field_map_path.addInputField(outPutFeatureClass, "Path")
        field_map_path.addInputField(target_layer, "path")
        
        field_map_name.addInputField(outPutFeatureClass, "Name")
        field_map_name.addInputField(target_layer, "name")

        photo_path = field_map_path.outputField
        photo_path.name = "Path"
        field_map_path.outputField = photo_path

        name_path = field_map_name.outputField
        name_path.name = "Name"
        field_map_name.outputField = name_path

        fieldMappings.addFieldMap(field_map_name)
        fieldMappings.addFieldMap(field_map_path)
        arcpy.Append_management(outPutFeatureClass, target_layer, "NO_TEST", fieldMappings)
        arcpy.SelectLayerByAttribute_management(target_layer, "NEW_SELECTION", "circuitid IS NULL")
        string = r'!path!.split("\\")[5]'
        arcpy.CalculateField_management(target_layer, "circuitid", string, "PYTHON3")
        arcpy.SelectLayerByAttribute_management(target_layer, "CLEAR_SELECTION")

    def run(self):
        self.run_geotagged()
        #self.append_to_photoPoints('C:\\Users\\CPenni3\\OneDrive - Duke Energy\\Documents\\ArcGIS\\Projects\\GeoTagged_PhotoPoints\\GeoTagged_PhotoPoints.gdb\\fc_01212403')


if __name__ == "__main__":
    env = ETL_Processing()
    env.run()
