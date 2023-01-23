import os

### Set Environment Variables
os.environ['envn'] = 'TEST'
os.environ['header'] = 'True'
os.environ['inferSchema'] = 'True'

### Get Environment Variables
envn = os.environ['envn']
header = os.environ['header']
inferSchema = os.environ['inferSchema']

### Set Other Variables
appName = "USA Report"
current_path = os.getcwd()

staging_dim_charges = current_path + '/../staging/dim/Charges/'
staging_dim_damages = current_path + '/../staging/dim/Damages/'
staging_dim_endorse = current_path + '/../staging/dim/Endorse/'
staging_dim_restrict = current_path + '/../staging/dim/Restrict/'
staging_fact_person = current_path + '/../staging/fact/Person/'
staging_fact_Unit = current_path + '/../staging/fact/Unit/'

