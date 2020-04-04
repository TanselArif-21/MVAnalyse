# Author     : Tansel.Arif
# Date       : 24-04-2019
# Title      : Python Helpers
# Description: This is a module comatining useful functions and
#              classes

#####################################################################
##                              Imports                            ##
#####################################################################
import pyodbc
import pandas as pd
import numpy as np
import os
import sqlalchemy
import pymssql
import matplotlib.pyplot as plt


#####################################################################
##                              Classes                            ##
#####################################################################


################################Database#############################

class Connection:
    '''
    This class deals with everything with connecting to the database
    and server, getting a connection and reusing that connection
    for subsequent queries.

    Example Usage:
    myConnection = Connection(myServer,mydb)
    myConnection.queryData("SELECT TOP 10 * FROM TABLE")
    '''
    
    def __init__(self,server,db):
        self.server = server
        self.db = db
        self.cur = None
    
    def getConnection(self):
        '''
        This function gets a connection using the server and db
        member variables
        '''
        
        # Get a connection to the db
        self.con = pyodbc.connect('Trusted_Connection=yes', driver = '{SQL Server}',server = self.server, database = self.db)

        # Set the encoding
        self.con.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')

        # Get a cursor
        self.cur = self.con.cursor()
        
    def queryData(self,query):
        '''
        This function uses the connection to query
        '''

        # If there is no cursur variable, get it
        if not self.cur:
            self.getConnection()

        # Execute the query
        self.cur.execute(query)

        # Get the results and store them for access later
        self.lastQueryResults = self.cur.fetchall()

        self.columns = [column[0] for column in self.cur.description]

        self.lastQueryResultsAsDF = pd.DataFrame([list(i) for i in self.lastQueryResults],columns = self.columns)
		
		
###################################Readers################################

class BaseReader:
	'''
	A basic reader that stores the data path

	Example Usage:

	Not intended to be used. Only to be inherited
	'''

	def __init__(self, dataPath):

		# Set the dataPath
		self.dataPath = dataPath

	def read(self):
		raise NotImplementedError
		
	def save(self):
		raise NotImplementedError


class ExcelReader(BaseReader):

	def __init__(self,dataPath,sheetName,headerRow = 0,outputPath = 'ReaderOutput.csv', outputColumns = []):
		BaseReader.__init__(self,dataPath)

		self.sheetName = sheetName
		self.headerRow = headerRow
		self.outputPath = outputPath
		self.outputColumns = outputColumns

	def readAllWorkbooks(self):
		# Get the data from csv files
		table = []
		files = os.listdir(self.dataPath)
		
		# Don't read the temporary excel files
		files = list(filter(lambda x: '$' not in x,files))
		
		print('\nReading from location: {}'.format(self.dataPath))
		
		i = 0		
		for name in files:
			i += 1
			table.append(pd.read_excel(os.path.join(self.dataPath,name),sheet_name = self.sheetName, header = self.headerRow))
			print('{} out of {} done.'.format(i,len(files)))

		self.dfs = table

		self.df = pd.concat(table, axis=0, sort = True)
		
	def save(self):
		if self.outputColumns:
			print('Saving specified columns to location {}'.format(self.outputPath))
			# Save to specified csv file
			self.df[self.outputColumns].to_csv(self.outputPath, index=False)
		else:
			print('Saving all columns to location {}'.format(self.outputPath))
			# Save to specified csv file
			self.df.to_csv(self.outputPath, index=False)

	def read(self):
		self.readAllWorkbooks()
		

class CSVReader(BaseReader):

	def __init__(self,dataPath,sheetName,headerRow = 0,outputPath = 'ReaderOutput.csv'):
		BaseReader.__init__(self,dataPath)

		self.sheetName = sheetName
		self.headerRow = headerRow
		self.outputPath = outputPath

	def readAllCSVs(self):
		# Get the data from csv files
		table = []
		files = os.listdir(self.dataPath)
		
		# Don't read the temporary excel files
		files = list(filter(lambda x: '$' not in x,files))
		
		i = 0
		for name in files:
			i += 1
			table.append(pd.read_csv(os.path.join(self.dataPath,name), header = self.headerRow))
			print('{} out of {} done.'.format(i,len(files)))

		self.dfs = table

		self.df = pd.concat(table, axis=0, sort = True)
		
	def save(self):
		# Save to specified csv file
		self.df.to_csv(self.outputPath, index=False)

	def read(self):
		self.readAllCSVs()
		
	
class MultipleExcelReader(ExcelReader):

	def __init__(self, dataPaths, sheetNames, headerRows = [0,0], 
		outputPath = 'AnonOutput.csv', outputColumns = []):
		
		self.outputPath = outputPath
		self.ExcelReaders = []
		self.dfs = []
		self.outputColumns = outputColumns
		
		for p,s,h in zip(dataPaths,sheetNames,headerRows):
			self.ExcelReaders.append(ExcelReader(p,s,h,outputPath))
		
	def readAllWorkbooks(self):
		# Read the data
		for i,reader in enumerate(self.ExcelReaders):
			print('\n\nReading {} of {} in location: {}'.format(i+1,len(self.ExcelReaders),reader.dataPath))
			reader.read()
			self.dfs.append(reader.df)
			
		self.df = pd.concat(self.dfs, axis=0, sort = True)
		
		
class ConnectionReader(Connection,ExcelReader):
	'''
	This class is to be used for reading in multiple UFLP csv files and writing to SQL server. The expected columns
	in the Excel/CSV files are:
	[Column1],[Column2]
	The table name is:
	[db1].[dbo].[tempTable]
	
	Usage Example:
	
	myConnectionReader = PythonHelpers.ConnectionReader(server=server,db=db,dataPath=dataPath,sheetName=sheetName,
                                         headerRow=headerRow,outputPath=outputPath)
										
	myConnectionReader.read()
	
	myConnectionReader.save()
	
	myConnectionReader.writeData()
	'''

	def __init__(self,server,db,dataPath,sheetName,headerRow = 0,outputPath='ReaderOutput.csv'):
		'''
		Constructor. Utilises the constructors of the inherited classes.
		
		:param server: A string representing the server
		:param db: A string representing the database name
		:param dataPath: A string representing the path to the folder containing the csv files
		:param sheetName: The name of the sheet which contains the data
		:param headerRow: The row index (counting from 0) of the header row
		:param outputPath: A string representing the output file including the path
		'''
		
		# Initialise this object as a Connection object
		Connection.__init__(self,server,db)
		
		# Initialise this object as an ExcelReader object
		ExcelReader.__init__(self,dataPath,sheetName,headerRow,outputPath)	
		
	def prepData(self):
		'''
		This method is used to prepare the data for writing to the sql db.
		'''
		
		# Replace any nan values with an empty string
		self.df = self.df.replace(np.nan, '', regex=True)
		
		
	def writeData(self):
		'''
		This method writes the dataframe on this object to the table on sql server
		'''
		
		# Prepare the data
		self.prepData()
		
		# If there is no cursur variable, get it
		if not self.cur:
			self.getConnection()
		
		# These are the columns of the table on sql server
		cols = '[Column1],[Column2]'
		
		# This is just constructing a string with a '?' for each column
		values = ('?,'*len(cols.replace('[','').replace(']','').split(',')))[0:-1]
		
		# These are the columns of the dataframe
		dfcols = '[Column1],[Column2]'
		#dfcols = cols
		
		# A counter to keep track of the insert count. The index in the for loop below resets every so often so we use this
		counter = 1
			
		# Iterate over the rows of the dataframe and insert each row into the table on sql server
		for index,row in self.df[dfcols.replace('[','').replace(']','').split(',')].iterrows():
		
			# Print progress
			if (index % 1000 == 0):
				print('{} out of {} done'.format(counter,self.df.shape[0]))
			
			# Increment the counter
			counter = counter + 1
			
			# Insert into the table
			self.cur.execute("INSERT INTO [db1].[dbo].[tempTable]({}) values ({})".format(cols,values), 
				row['Column1'],row['Column2'])
		
		# Only commit if the entire dataframe has successfully been inserted
		self.con.commit()
		
		print('Complete!')
		
		# Close connections so that the table can be accessed by other connections
		self.cur.close()
		self.con.close()
		
	def save(self):
		'''
		This method saves the dataframe on this object to a file
		'''
		
		# Prepare the data
		self.prepData()
		
		# Save the dataframe to a file
		ExcelReader.save(self)



#####################################################################
##                            Functions                            ##
#####################################################################

##############################Plotting###############################

def getFigureAndSingleAxis(title = '',titleFontSize = 30,xlabelFontSize = 20,ylabelFontSize = 20,xtickSize = 10,ytickSize = 10,xVertical = False,yVertical = False, figsize = (30,10)):
	fig = plt.figure(figsize=figsize)
	ax = fig.add_axes([0,0,1,1])

	# label and tick sizes
	ax.set_title(title, fontsize=titleFontSize)
	ax.set_xlabel('', fontsize=xlabelFontSize)
	ax.set_ylabel('', fontsize=ylabelFontSize)

	for tick in ax.xaxis.get_major_ticks():
		tick.label.set_fontsize(xtickSize) 
		if xVertical:
			tick.label.set_rotation('vertical')

	for tick in ax.yaxis.get_major_ticks():
		tick.label.set_fontsize(ytickSize) 
		if yVertical:
			tick.label.set_rotation('horizontal')
			
	return fig,ax