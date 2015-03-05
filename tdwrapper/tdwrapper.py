import subprocess32 as subprocess
import pandas as pd

# TODO
# Read dbc.columns to set the field width and proper cast (date, float)
# Implement FastLoad
# Delete logtable when there is an error in FastExport

class tdwrapper(object):

    def __init__(self, logon_string_file, log_database, shell='/bin/bash'):

        self.logon_string_file = logon_string_file
        self.log_database = log_database
        self.shell = shell

        logon_string = open(logon_string_file).readline()
        self.userid = logon_string[(logon_string.find('/') + 1):logon_string.find(',')].split(' ')[-1]

        self.bteq_script = ''
        self.bteq_log = ''
        self.bteq_err = ''

        self.fexp_script = ''
        self.fexp_log = ''
        self.fexp_err = ''

    def __get_colnames(self, table_name):

        # Initialize bteq script and output
        self.bteq_script = ''
        self.bteq_log = ''
        self.bteq_err = ''

        # Check if table name does not exceed 30 characters
        table_name_split = table_name.split('.')

        if len(table_name_split[1]) > 30:
            raise Exception('The length of table name is greater than 30 characters.')

        # Set BTEQ output file name
        file_name_bteq = '.tdwrapper.get_colnames.{}.tmp'.format(table_name)

        # Write BTEQ script
        self.bteq_script = (
            '.RUN FILE ' + self.logon_string_file + ';\n'
            '.SET TITLEDASHES OFF;\n'
            '.EXPORT REPORT FILE=' + file_name_bteq + ';\n'
            'select columnname (title \'\') from dbc.columns\n'
            'where databasename=\n'
            '\'' + table_name_split[0] + '\'\n'
            'and tablename=\n'
            '\'' + table_name_split[1] + '\'\n'
            'order by columnid;\n'
            '.EXPORT RESET;\n'
            '.EXIT;\n'
            )

        # Delete BTEQ output file, if exists, before calling BTEQ
        subprocess.call('if [ -f {} ]; then rm {}; fi;'.format(file_name_bteq, file_name_bteq), shell=True, executable=self.shell)

        # Execute BTEQ script
        p = subprocess.Popen('bteq', stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        p.stdin.write(self.bteq_script)
        (self.bteq_log, self.bteq_err) = p.communicate()

        # Check if BTEQ run is successful
        if p.returncode is not 0:
            raise Exception('fexp returns {}. See bteq_log attribute for details.'.format(p.returncode))

        # Read BTEQ output file as list
        f = open(file_name_bteq)
        lines = [line.strip() for line in f.readlines()]
        f.close()

        # Delete BTEQ output file
        subprocess.call('if [ -f {} ]; then rm {}; fi;'.format(file_name_bteq, file_name_bteq), shell=True, executable=self.shell)

        # Return list
        return lines

    def to_csv(self, sql, file_name, delim='|', max_field_buffer=256):
        
        # Initialize bteq script and output
        self.fexp_script = ''
        self.fexp_log = ''
        self.fexp_err = ''

        # If delim = '\t', replace it with 'x\'09\''
        if delim is '\t':
            delim = 'x\'09\''

        # Parse SQL statement
        sql_split = sql.strip().split(' ')

        select_command = sql_split[0]
        if select_command.lower() not in ['select', 'sel']:
            raise Exception('No \'select\' command is found in sql query.')

        try:
            from_command_index = map(lambda token: token.lower(), sql_split).index('from')
        except ValueError:
            raise Exception('No \'from\' command is found in sql query.')

        column_names = ' '.join(sql_split[1:from_command_index]).split(',')

        table_name = sql_split[from_command_index + 1].strip()

        sql_rest = ' '.join(sql_split[from_command_index:])

        # If SQL is 'select * from ...', retrieve column names using BTEQ
        if len(column_names) == 1 and column_names[0].strip() == '*':
            column_names = self.__get_colnames(table_name)

        # Set FastExport output file name
        file_name_fexp = '.tdwrapper.{}.tmp'.format(table_name)

        # Write FastExport script
        column_header = ['\'{}\''.format(column_name.strip()) for column_name in column_names]
        column_header = '||\'{}\'||'.format(delim).join(column_header)
        
        column_names_fexp = map(lambda column_name: 'coalesce(cast({} as VARCHAR({})),\'?\')'.format(column_name.strip(), max_field_buffer), column_names)
        column_names_fexp = '\n||\'{}\'||\n'.format(delim).join(column_names_fexp)

        self.fexp_script = (
            '.LOGTABLE ' + self.log_database + '.' + self.userid + '_fexp_log;\n'
            '.RUN FILE ' + self.logon_string_file + ';\n'
            '.BEGIN EXPORT;\n'
            '.EXPORT MODE RECORD FORMAT TEXT OUTFILE ' + file_name_fexp + ';\n'
            'select * from (select ' + column_header + ' as x) a;\n'
            '' + '\n'.join([select_command, column_names_fexp, sql_rest]) + ';\n'
            '.END EXPORT;\n'
            '.LOGOFF;\n'
            )

        # Delete FastExport output file, if exists, before calling FastExport
        subprocess.call('if [ -f {} ]; then rm {}; fi;'.format(file_name_fexp, file_name_fexp), shell=True, executable=self.shell)

        # Execute FastExport script
        p = subprocess.Popen('fexp', stdin=subprocess.PIPE, stdout=subprocess.PIPE)
        p.stdin.write(self.fexp_script)
        (self.fexp_log, self.fexp_err) = p.communicate()

        # Check if FastExport run is successful
        if p.returncode is not 0:
            raise Exception('fexp returns {}. See fexp_log attribute for details.'.format(p.returncode))

        # Cut the first two bytes off
        subprocess.call('if [ -f {} ]; then cut -b 3- {} > {}; fi;'.format(file_name_fexp, file_name_fexp, file_name), shell=True, executable=self.shell)

        # Delete FastExport output file
        subprocess.call('if [ -f {} ]; then rm {}; fi;'.format(file_name_fexp, file_name_fexp), shell=True, executable=self.shell)

    def to_dataframe(self, sql, file_name=None, delim='|', max_field_buffer=256):
        
        # Set filename to pass to to_csv()
        file_name_to_csv = file_name

        if file_name is None:
            file_name_to_csv = '.tdwrapper.to_dataframe.tmp'
        
        # Call to_csv() to export data to a file
        self.to_csv(sql, file_name_to_csv, delim=delim, max_field_buffer=max_field_buffer)

        # Read the final output file as DataFrame
        df = pd.read_csv(file_name_to_csv, sep=delim, na_values='?')
        
        # Delete final output file, if file name is not given by user
        if file_name is None:
            subprocess.call('if [ -f {} ]; then rm {}; fi;'.format(file_name_to_csv, file_name_to_csv), shell=True, executable=self.shell)

        # Return data frame
        return df

    # def from_dataframe(self, df, primary_index_columns, table_name, file_name):
        
    #     file_name = ''
    #     fastload_script = (
    #         'RUN ' + self.logon_string_file + ';\n'
    #         'drop table ' + self.log_database + '.' + self.userid + '_fload_err1;\n'
    #         'drop table ' + self.log_database + '.' + self.userid + '_fload_err2;\n'
    #         'drop table ' + table_name + ';\n'
    #         'create table ' + table_name + '(\n'
    #         '    TODO,\n'
    #         '    TODO\n'
    #         ') primary index (' + 'TODO' + ');\n'
    #         'SET RECORD VARTEXT;\n'
    #         'DEFINE\n'
    #         '    TODO,\n'
    #         '    TODO\n'
    #         '    FILE = ' + file_name + ';\n'
    #         'BEGIN LOADING ' + table_name + '\n'
    #         '    ERRORFILES ' + self.log_database + '.' + self.userid + '_fload_err1, ' + self.log_database + '.' + self.userid + '_fload_err2;\n'
    #         'INSERT INTO ' + table_name + ' VALUES (\n'
    #         '    :TODO,\n'
    #         '    :TODO\n'
    #         ');\n'
    #         '.END LOADING;\n'
    #         '.LOGOFF;\n'
    #     )