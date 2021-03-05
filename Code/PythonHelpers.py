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
		
	
		

################################Anonymisation#############################

class AnonymiserExcel:
    '''
    This class deals with all aspects of anonymising a dataset from excel spreadsheets. It takes
    care of the reading in of all excel spreadsheets in a specified folder, it anonymises the sensitive
    id and removes the specified sensitive columns and outputs one big dataframe to a csv file of choice.

    Example Usage:

    sheetname = 'Sheet1'

    sensitiveCols = set(['Candidate Identifier','Contact#','Email','Employee Number','Name','Req. Creation Date',
                     'Submission Identifier','Did you participate in any Unilever programme before'])
                     
    myAnonymiser = AnonymiserExcel(dataPath=Directory.dataPath,sheetName=sheetname, 
                                             anonymisation_column='Candidate Identifier', sensitiveCols=sensitiveCols,
                                             headerRow=2, outputPath=os.path.join(Directory.dataPath,'AnonOutput.csv'))

    myAnonymiser.readAnonymiseSave()
    '''

    def __init__(self, dataPath, sheetName, anonymisation_column = 'Candidate Identifier', 
    	numeric_columns = ['Gamification Score','Hirevue Insight Scores'], sensitiveCols = set(), 
    	headerRow = 0, outputPath = 'AnonOutput.csv'):
        self.dataPath = dataPath
        self.sheetName = sheetName
        self.headerRow = headerRow
        self.anonymisation_column = anonymisation_column
        self.sensitiveCols = sensitiveCols | {anonymisation_column}
        self.outputPath = outputPath
        self.numeric_columns = numeric_columns

    def readAllWorkbooks(self):
        # Get the data from csv files
        table = []
        files = os.listdir(self.dataPath)
        i = 0
        for name in files:   
            i += 1
            table.append(pd.read_excel(os.path.join(self.dataPath,name),sheet_name = self.sheetName, header = self.headerRow))
            print('{} out of {} done.'.format(i,len(files)))

        self.dfs = table

        self.df = pd.concat(table, axis=0, sort = True)

    def anonymise(self):

        # Get unique candidate ids
        unique_list = self.df[self.anonymisation_column].unique()

        # Get a candidate mapping dataframe
        candidate_mapping = pd.concat([pd.DataFrame(unique_list,columns=[self.anonymisation_column]),
                              pd.DataFrame(list(range(len(unique_list))),columns=['Anon_ID'])],axis=1)

        # Merge onto original dataframe
        self.dfWithAnonID = pd.merge(left=self.df,right=candidate_mapping,on=self.anonymisation_column)

        # Get the columns that are not sensitive
        self.acceptedCols = set(self.dfWithAnonID.columns) - self.sensitiveCols

        # Round to nearest integer
        for col in self.numeric_columns:
            self.dfWithAnonID[col] = self.dfWithAnonID[col].apply(lambda x: np.round(x,0) if x is not None else None)

        # Anonymise
        self.df_anonymised = self.dfWithAnonID[list(self.acceptedCols)]

    def save(self):

        # Save to specified csv file
        self.df_anonymised.to_csv(self.outputPath, index=False)
		
    def save_pre_anonymised(self):
		
        # Save the dataframe before it was anonymised
        self.df_anonymised.to_csv(self.outputPath.replace('.csv','_pre_anonymised.csv'), index=False)

    def readAnonymise(self):

        self.readAllWorkbooks()
        self.anonymise()

    def readAnonymiseSave(self):

        self.readAllWorkbooks()
        self.anonymise()
        self.save()

        
class FullAnonymiser:
	'''
	This class builds on the AnonymiserExcel (and others in the future). This class is an encapsulation of objects of
	other classes. The main purpose of this class is to dispatch multiple Anonymisers to different folders with 
	different column names etc... but intended to be within the same project and part of the same dataset. They may
	have just been extracted differently.

	Example Usage:
	
	# Set the names of the sheets of the first and second directory respectively
	sheetnames = ['UFLP Candidate Detail Report','Sheet1']

	# Set which column names are sensitive and are to be removed automatically
	sensitiveCols = set(['Candidate Number','Submission Identifier','First Name','Last Name','Mobile Phone','Email','City of Residence',
	                     'State/Prov of Residence','DOB'])

	# Set the locations of the two data locations
	data1 = os.path.join(Directory.dataPath,'Data1')
	data2 = os.path.join(Directory.dataPath,'Data2')

	# Create a FullAnonymiser object and specify the two data locations and the column that is to be anonymised for each location
	myFullAnonymiser = PythonHelpers.FullAnonymiser([data1,data2],sheetNames=sheetnames, 
	                                             anonymisation_columns=['Candidate Number','Candidate Identifier'],
	                                                 numeric_columns = [['Gamification Scores','Hirevue Insight Scores'],['Gamification Score','Hirevue Insight Scores']],
	                                            sensitiveCols=sensitiveCols,
	                                             headerRows=[4,2], outputPath=os.path.join(Directory.outputPath,'AnonOutput.csv'))

	# Read from the respective locations and save it into the specified folder
	myFullAnonymiser.readAnonymiseSave()
	'''

	def __init__(self, dataPaths, sheetNames, anonymisation_columns = [], 
		numeric_columns = [['Gamification Score','Hirevue Insight Scores'],['Gamification Score','Hirevue Insight Scores']], 
		sensitiveCols = set(), headerRows = [0,0], 
		outputPath = 'AnonOutput.csv', outputColumns = []):
		'''
		Constructor

		:param dataPaths: a list of data paths each corresponding to a location with many excel workbooks
		:param sheetNames: a list of names for each data path
		:param anonymisation_columns: for each of the data paths, the column to anonymise
		:param numeric_columns: the numeric columns to be rounded (a list of columns per data path)
		:param sensitiveCols: a list of columns that are sensitive. Only one list. Doesn't have to exist as columns in both paths
		:param headerRows: the rows to be considered as header rows. An integer per data path
		:param outputPath: The path to the output file including the desired file name and extension
		'''
		
		# This is to be a list of Anonymiser objects
		self.anons = []
		
		# Save the paths to the data locations to this object
		self.dataPaths = dataPaths
		
		# Save the output path to this object
		self.outputPath = outputPath
		
		self.outputColumns = outputColumns
		
		# Instantiate an anonymiser object for each data path. Store these in the anons list on this object
		for i in range(len(self.dataPaths)):
			self.anons.append(AnonymiserExcel(self.dataPaths[i],sheetName=sheetNames[i],anonymisation_column = anonymisation_columns[i],
				numeric_columns = numeric_columns[i],sensitiveCols = sensitiveCols,headerRow = headerRows[i], 
				outputPath = self.outputPath))

	def readAnonymiseSave(self):
		'''
		This method utilises bot the readAnonymise method and the Save method on this object
		'''

		# Read all the files in and anaonymise them
		self.readAnonymise()
		
		if self.outputColumns:
			columns = ['Anon_ID'] + list(set(self.outputColumns) - {'Anon_ID'})
		else:
			columns = ['Anon_ID'] + list(set(self.df.columns) - {'Anon_ID'})

		# Set the Anon_ID column to be the first column
		self.df[columns].to_csv(self.outputPath, index=False)


	def readAnonymise(self):
		'''
		This method reads all the files and anonymises them by triggering the readAnonymise methods of
		the Anonymise objects stored in the anons list on this object
		'''

		# This is going to store each of the dataframes from the anonymise objects (1 per data path)
		self.dfs = []

		# Loop through all the anonymise objects and call the readAnonymise methods on them
		for i in range(len(self.dataPaths)):
		
			print('\n\nReading and Anonymising directory {} out of {}: Location - {} \n'.format(i + 1,len(self.dataPaths),self.dataPaths[i]))
			self.anons[i].readAnonymise()
			self.dfs.append(self.anons[i].df_anonymised.copy())

		# Loop through all the dataframes on this object and anonymise. Keep track of the max id so that we don't start the
		# anonymise ids from 0 again
		for i in range(len(self.dfs)):
			if i >= 1:
				maxId = max(self.dfs[i-1]['Anon_ID'])
			else:
				maxId = 0
			
			# Each respective anonymise object will anonymise it's own ids. But they all start from 0 so we offset deopending on the
			# last id
			self.dfs[i].loc[:,'Anon_ID'] = self.dfs[i]['Anon_ID'].apply(lambda x: x + maxId + 1)


		self.df = pd.concat(self.dfs, axis=0, sort = True)

		self.df = self.df[['Anon_ID'] + list(set(self.df.columns) - {'Anon_ID'})]
		
		print('Output CSV saved in location: {}'.format(self.outputPath))


################################Project Specific#############################

class PymetricsAnonymiser(FullAnonymiser):
	'''
	This class is a Pymetrics specific anonymiser and is used to anonymise the data for pymetrics
	so that we send the same dataset to Pymetrics

	Example Usage:
	
	# Set the names of the sheets of the first and second directory respectively
	sheetnames = ['UFLP Candidate Detail Report','Sheet1']

	# Set which column names are sensitive and are to be removed automatically
	sensitiveCols = set(['Candidate Number','Submission Identifier','First Name','Last Name','Mobile Phone','Email','City of Residence',
	                     'State/Prov of Residence','DOB'])

	# Set the locations of the two data locations
	data1 = os.path.join(Directory.dataPath,'Data1')
	data2 = os.path.join(Directory.dataPath,'Data2')

	# Create a FullAnonymiser object and specify the two data locations and the column that is to be anonymised for each location
	myFullAnonymiser = PythonHelpers.FullAnonymiser([data1,data2],sheetNames=sheetnames, 
	                                             anonymisation_columns=['Candidate Number','Candidate Identifier'],
	                                                 numeric_columns = [['Gamification Scores','Hirevue Insight Scores'],['Gamification Score','Hirevue Insight Scores']],
	                                            sensitiveCols=sensitiveCols,
	                                             headerRows=[4,2], outputPath=os.path.join(Directory.outputPath,'AnonOutput.csv'))

	# Read from the respective locations and save it into the specified folder
	myFullAnonymiser.readAnonymiseSave()
	'''

	def readAnonymise(self):
		'''
		In addition to the base behaviour, this method truncates the req creation date to year only
		'''

		# Base behaviour
		FullAnonymiser.readAnonymise(self)

		# Get the year of the req. creation date field
		self.df['Req. Creation Date'] = self.df['Req. Creation Date'].apply(lambda x: x.year)
		
		
class UFLPConnectionReader(Connection,ExcelReader):
	'''
	This class is to be used for reading in multiple UFLP csv files and writing to SQL server. The expected columns
	in the Excel/CSV files are:
	[Candidate Number],[Country],[Country of Residence],[Current Status Name],[DC Completed Date],[Did you participate in any Unilever programme before],[Discovery Centre Status],[EU Opted In/Out],[Ethnicity],[Function],[Gamification Scores],[Gamification Status],[Gender],[Hired Date],[Hirevue Assessor Decision],[Hirevue Insight Scores],[Hirevue Insight Status],[First Name],[Offer Status],[Offered Date],[Profile Source],[Profile Source Type],[Req. Creation Date],[Requisition Number],[Select your work experience],[Submission Completed Date],[Submission Identifier],[Time to Hire],[Time to offer accepted]
	The table name is:
	[csdd927a].[dbo].[TA_UFLPData]
	
	Usage Example:
	
	myUFLPConnectionReader = PythonHelpers.UFLPConnectionReader(server=server,db=db,dataPath=dataPath,sheetName=sheetName,
                                         headerRow=headerRow,outputPath=outputPath)
										
	myUFLPConnectionReader.read()
	
	myUFLPConnectionReader.save()
	
	myUFLPConnectionReader.writeData()
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
		
		# Convert the candidate identifiers to a string
		self.df['Candidate Identifier'] = self.df['Candidate Identifier'].apply(lambda x: str(x))
		
		# Truncate Name fields to 100 characters
		self.df['Name'] = self.df['Name'].apply(lambda x: str(x[0:100]))
		
		#self.df['Candidate Number'] = self.df['Candidate Number'].apply(lambda x: str(x))
		
		
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
		cols = '[Candidate Number],[Country],[Country of Residence],[Current Status Name],[DC Completed Date],[Did you participate in any Unilever programme before],[Discovery Centre Status],[EU Opted In/Out],[Ethnicity],[Function],[Gamification Scores],[Gamification Status],[Gender],[Hired Date],[Hirevue Assessor Decision],[Hirevue Insight Scores],[Hirevue Insight Status],[First Name],[Offer Status],[Offered Date],[Profile Source],[Profile Source Type],[Req. Creation Date],[Requisition Number],[Select your work experience],[Submission Completed Date],[Submission Identifier],[Time to Hire],[Time to offer accepted]'
		
		# This is just constructing a string with a '?' for each column
		values = ('?,'*len(cols.replace('[','').replace(']','').split(',')))[0:-1]
		
		# These are the columns of the dataframe
		dfcols = '[Candidate Identifier],[Country],[Country of Residence],[Current Status],[DC Completed Date],[Did you participate in any Unilever programme before],[Discovery Centre Status],[EU Opted In/Out],[Ethnicity],[Function],[Gamification Score],[Gamification Status],[Gender],[Hired Date],[Hirevue Assessor Decision],[Hirevue Insight Scores],[Hirevue Insight Status],[Name],[Offer Status],[Offered Date],[Profile Source],[Profile Source Type],[Req. Creation Date],[Req. Identifier],[Select your work experience],[Submission Completed Date],[Submission Identifier],[Time to Hire],[Time to offer accepted]'
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
			self.cur.execute("INSERT INTO [csdd927a].[dbo].[TA_UFLPData]({}) values ({})".format(cols,values), 
				row['Candidate Identifier'],row['Country'],row['Country of Residence'],row['Current Status'],row['DC Completed Date'],row['Did you participate in any Unilever programme before'],row['Discovery Centre Status'],row['EU Opted In/Out'],row['Ethnicity'],row['Function'],row['Gamification Score'],row['Gamification Status'],row['Gender'],row['Hired Date'],row['Hirevue Assessor Decision'],row['Hirevue Insight Scores'],row['Hirevue Insight Status'],row['Name'],row['Offer Status'],row['Offered Date'],row['Profile Source'],row['Profile Source Type'],row['Req. Creation Date'],row['Req. Identifier'],row['Select your work experience'],row['Submission Completed Date'],row['Submission Identifier'],row['Time to Hire'],row['Time to offer accepted'])
				#row['Candidate Number'],row['Country'],row['Country of Residence'],row['Current Status Name'],row['DC Completed Date'],row['DOB'],row['Did you participate in any Unilever programme before'],row['Discovery Centre Status'],row['EU Opted In/Out'],row['Email'],row['Ethnicity'],row['Function'],row['Gamification Scores'],row['Gamification Status'],row['Gender'],row['Hired Date'],row['Hirevue Assessor Decision'],row['Hirevue Insight Scores'],row['Hirevue Insight Status'],row['First Name'],row['Offer Status'],row['Offered Date'],row['Profile Source'],row['Profile Source Type'],row['Req. Creation Date'],row['Requisition Number'],row['Select your work experience'],row['Submission Completed Date'],row['Submission Identifier'],row['Time to Hire'],row['Time to offer accepted'])
		
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