import wget
import zipfile
import psycopg2
import shapefile
import json
import sys

class DATTools:

    def __init__(self):
        #Contructor
        pass


    def getShapeFile(self, location):
        print("Baixando arquivos...")
        try:
            filename = wget.download(location)
            zip_ref = zipfile.ZipFile(filename, 'r')
            zip_ref.extractall('.')
            zip_ref.close()            
        except Exception as error:
            print("Não foi possível baixar os arqivos. Verifique sua conexão.")
            exit()
        return 'deter_public.shp'

    def writeNewShapeFile(self, filename, outputName):
        print("\nCriando Shapefile com os dados selecionados...")
        try:
            sf = shapefile.Reader(filename)
            newShp = shapefile.Writer(outputName, shapeType=5)
            fields = sf.fields[1:]
            newShp.fields = fields
            
            totalTasks = len(sf)
            taskCount  = 1
            for d in sf.iterShapeRecords():
                className = d.record['CLASSNAME']
                if className == 'DESMATAMENTO_CR' or className == 'DESMATAMENTO_VEG' or className == 'MINERACAO':
                    newShp.record(*d.record)
                    newShp.shape(d.shape)# d.shape.points
                self.progressBar(taskCount, totalTasks)
                taskCount += 1                
            newShp.close()
            print()
        except IOError:
            print("Error durante a escrita do Shapefile.")
            exit()
        return True

    def sendToPostgis(self, fileName, tableName):
        print("Enviando dados para o PostGis.")
        #Open File. Read File. Loads like JSON
        jsonData = json.loads(open("login.json", "r").read())

        # Set up the datbase connection
        connection = psycopg2.connect(host=jsonData["host"], user=jsonData["user"], password=jsonData["password"])

        # Get the database cursor to execute queries
        cursor = connection.cursor()

        r = shapefile.Reader(fileName)

        # Build a query to create our "cities" table
        # with an id and geometry column.
        table_query = "CREATE TABLE {} (id SERIAL, PRIMARY KEY (id), geom GEOMETRY(MULTIPOLYGON, 4674),".format(tableName)

        fields = r.fields[1:]
        # We are going to keep track of the fields as a
        # string. We'll use this query fragment for our
        # insert query later.
        field_string = ""

        # Loop throug the fields and build our table
        # creation query.
        for i in range(len(fields)):
            # get the field information
            f = fields[i]
            # get the field name and lowercase it for consistency
            f_name = f[0].lower()
            # add the name to the query and our string list of fields
            table_query += f_name
            field_string += f_name
            # Get the proper type for each field. Note this check
            # is not comprehensive but it convers the types in
            # our sample shapefile.
            if f[1] == "F":
                table_query += " DOUBLE PRECISION"
            elif f[1] == "N":
                table_query += " NUMERIC"
            else:
                table_query += " VARCHAR"
            # If this field isn' the last, we'll add a comma
            # and some formatting.
            if i != len(fields) - 1:
                table_query += ","
                table_query += "\n"
                table_query += "                 "
                field_string += ","
            # Close the query on the field.
            else:
                table_query += ")"
                field_string += ",geom"

        # Execute the table query
        # print(table_query)

        # Execute the table query
        try:
            cursor.execute(table_query)
        except psycopg2.Error as error:
            # print("Meu erro {}".format(error))            
            if str(error).endswith("already exists\n"):
                print("A tabela informa já existe. Efetuando o Merge dos dados!")

        # Commit the change to the database or it won't stick.
        # PostgreSQL is transactional which is good so nothing
        # is stored until you're sure it works.
        
        connection.commit()  
        # Create a python generator for the
        # shapefiles shapes and records
        shape_records = (shp_rec for shp_rec in r.iterShapeRecords())

        # Loop through the shapefile data and add it to our new table.
        totalTasks = len(r)
        taskCount  = 1
        for sr in shape_records:
            # Get our point data.
            shape = sr.shape
            # First Points
            x, y = shape.points[0]
            poly = str(shape.points).replace("[", "").replace("]", "").replace(",", "").replace(") (", ",")

            # Get the attribute data and set it up as
            # a query fragment.
            attributes = ""
            for r in sr.record:
                if type(r) == type("string"):
                    r = r.replace("'", "''")
                attributes += "'{}'".format(r)
                attributes += ","

            # Build our insert query template for this shape record.
            # Notice we are going to use a PostGIS function
            # which can turn a WKT geometry into a PostGIS
            # geometry.
            point_query = """INSERT INTO {} ({}) VALUES ({} ST_Multi (ST_GeomFromText('POLYGON({})', 4674)))"""

            # Populate our query template with actual data.
            format_point_query += str(point_query).format(tableName, field_string, attributes, poly)

            try:
                if (taskCount % 10) == 0:
                    cursor.execute(format_point_query)
                    format_point_query = ''
                    connection.commit()
            except psycopg2.Error as error:
                connection.rollback()

                if str(error).startswith("geometry contains non-closed rings"):
                    try:
                        point_query = """INSERT INTO {} ({}) VALUES ({} ST_Multi(ST_GeomFromText('POLYGON({})', 4674)))"""
                        format_point_query = str(point_query).format(tableName, field_string, attributes,
                                                                     poly.replace(")", ", {} {})".format(x, y)))
                        cursor.execute(format_point_query)
                        connection.commit()
                    except psycopg2.Error as subError:
                        print("Não foi possível enviar para o PostGis.")
                        exit()
            self.progressBar(taskCount, totalTasks)
            taskCount += 1
        # Everything went ok so let's update the database.
        print()        
        connection.close()
        print("Finalizado com sucesso.")

    def progressBar(self, taskCount, totalTasks):
        percent = (taskCount * 100) / totalTasks
        sys.stdout.write("\rProcessando... %d%% concluídos" % percent)
        sys.stdout.flush()
