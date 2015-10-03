import subprocess32 as subprocess
import pandas as pd

#TODO resturcture the code to re-use to_fexp_script()
#TODO allow option to append header or not
#TODO Chcek if sel * from with more spaces work
#TODO Raise error when table does not exists in bteq process
#TODO Make log table name to be variable to support multiple connection

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

        self.fload_script = ''
        self.fload_log = ''
        self.fload_err = ''

    def __get_column_info(self, table_name, max_frac_digits_for_float):

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
            'select cast(ColumnName as varchar(64)) ||\'|\'|| cast(ColumnType as varchar(2)) ||\'|\'|| cast(ColumnFormat as varchar(64)) ColumnInfo from dbc.columnsv\n'
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
        p = subprocess.Popen('bteq', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p.stdin.write(self.bteq_script)
        (self.bteq_log, self.bteq_err) = p.communicate()

        # Check if BTEQ run is successful
        if p.returncode is not 0:
            print self.bteq_log
            print self.bteq_err
            raise Exception('bteq returns {}.'.format(p.returncode))

        # Read BTEQ output file as list

        with open(file_name_bteq, 'r') as f:
            next(f)
            lines = [line.strip().split('|') for line in f]

        # Delete BTEQ output file
        subprocess.call('if [ -f {} ]; then rm {}; fi;'.format(file_name_bteq, file_name_bteq), shell=True, executable=self.shell)

        column_info = []
        for line in lines:
            if line[1] == 'F':
                column_info += [(line[0], 'cast({} as decimal(38, {}))'.format(line[0], max_frac_digits_for_float), 40)]
            elif line[1] == 'D':
                column_info += [(line[0], line[0], len(line[2]) + 2)]
            elif line[1] in ['CF', 'CV']:
                column_info += [(line[0], line[0], int(line[2][line[2].find('(') + 1:line[2].find(')')]))]
            elif line[1] == 'DA':
                column_info += [(line[0], line[0], len(line[2]))]
            elif line[1] in ['I1', 'I2', 'I', 'I8']:
                column_info += [(line[0], line[0], int(line[2][line[2].find('(') + 1:line[2].find(')')]) + 2)]
            else:
                raise Exception('Do not know how to handle {} with data type {}.'.format(line[0], line[1]))

        # Return list
        return column_info

    def to_csv(self, sql, file_name, delim='|', max_frac_digits_for_float=4, print_stdout=False):
        
        # Initialize bteq script and output
        self.fexp_script = ''
        self.fexp_log = ''
        self.fexp_err = ''

        # If delim = '\t', replace it with 'x\'09\''
        if delim is '\t':
            delim = 'x\'09\''

        # Parse SQL statement
        sql_split = sql.strip().split()

        select_command = sql_split[0]
        if select_command.lower() not in ['select', 'sel']:
            raise Exception('No \'select\' command is found in sql query.')

        try:
            from_command_index = map(lambda token: token.lower(), sql_split).index('from')
        except ValueError:
            raise Exception('No \'from\' command is found in sql query.')

        column_names = [column_name.strip() for column_name in ' '.join(sql_split[1:from_command_index]).split(',')]
        table_name = sql_split[from_command_index + 1].strip()
        sql_rest = ' '.join(sql_split[from_command_index:])

        # Retreive column info
        column_info = self.__get_column_info(table_name, max_frac_digits_for_float)

        # If column names are specified by user, check against the column info
        if len(column_names) != 1 or column_names[0].strip() != '*':
            column_info_dict = dict([(item[0].lower(), item[1:])  for item in column_info])

            column_info = []
            for column_name in column_names:
                if column_name.lower() in column_info_dict:
                    column_info += [(column_name, ) + column_info_dict[column_name.lower()]]
                else:
                    raise Exception('Column {} does not exists in {}.'.format(column_name, table_name))     

        # Set FastExport output file name
        file_name_fexp = file_name + '.tmp'

        # Write FastExport script
        column_header = ['\'{}\''.format(header_name.strip()) for header_name, column_name, column_size in column_info]
        column_header = '||\'{}\'||'.format(delim).join(column_header)
        
        column_names_fexp = map(lambda (header_name, column_name, column_size): 'coalesce(cast({} as VARCHAR({})),\'?\')'.format(column_name.strip(), column_size), column_info)
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
        p = subprocess.Popen('fexp', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p.stdin.write(self.fexp_script)

        if print_stdout:
            p.stdin.flush()
            # Now start grabbing output.
            while p.poll() is None:
                l = p.stdout.readline()
                print l
            print p.stdout.read()

        (self.fexp_log, self.fexp_err) = p.communicate()

        # Check if FastExport run is successful
        if p.returncode is not 0:
            print self.fexp_log
            print self.fexp_err
            raise Exception('fexp returns {}.'.format(p.returncode))

        # Cut the first two bytes off
        subprocess.call('if [ -f {} ]; then cut -b 3- {} > {}; fi;'.format(file_name_fexp, file_name_fexp, file_name), shell=True, executable=self.shell)

        # Delete FastExport output file
        subprocess.call('if [ -f {} ]; then rm {}; fi;'.format(file_name_fexp, file_name_fexp), shell=True, executable=self.shell)

    def to_fexp_script(self, sql, script_filename, data_filename, header_filename, delim='|', max_frac_digits_for_float=4):
        
        # Initialize bteq script and output
        self.fexp_script = ''

        # If delim = '\t', replace it with 'x\'09\''
        if delim is '\t':
            delim = 'x\'09\''

        # Parse SQL statement
        sql_split = sql.strip().split()

        select_command = sql_split[0]
        if select_command.lower() not in ['select', 'sel']:
            raise Exception('No \'select\' command is found in sql query.')

        try:
            from_command_index = map(lambda token: token.lower(), sql_split).index('from')
        except ValueError:
            raise Exception('No \'from\' command is found in sql query.')

        column_names = [column_name.strip() for column_name in ' '.join(sql_split[1:from_command_index]).split(',')]
        table_name = sql_split[from_command_index + 1].strip()
        sql_rest = ' '.join(sql_split[from_command_index:])

        # Retreive column info
        column_info = self.__get_column_info(table_name, max_frac_digits_for_float)

        # If column names are specified by user, check against the column info
        if len(column_names) != 1 or column_names[0].strip() != '*':
            column_info_dict = dict([(item[0].lower(), item[1:])  for item in column_info])

            column_info = []
            for column_name in column_names:
                if column_name.lower() in column_info_dict:
                    column_info += [(column_name, ) + column_info_dict[column_name.lower()]]
                else:
                    raise Exception('Column {} does not exists in {}.'.format(column_name, table_name))

        # Write FastExport script
        column_header = ['\'{}\''.format(header_name.strip()) for header_name, column_name, column_size in column_info]
        column_header = '||\'{}\'||'.format(delim).join(column_header)
        
        column_names_fexp = map(lambda (header_name, column_name, column_size): 'coalesce(cast({} as VARCHAR({})),\'?\')'.format(column_name.strip(), column_size), column_info)
        column_names_fexp = '\n||\'{}\'||\n'.format(delim).join(column_names_fexp)

        self.fexp_script = (
            '.LOGTABLE ' + self.log_database + '.log_fexp_' + table_name.split('.')[1] + ';\n'
            '.RUN FILE ' + self.logon_string_file + ';\n'
            '.BEGIN EXPORT;\n'
            '.EXPORT MODE RECORD FORMAT TEXT OUTFILE ' + data_filename + ';\n'
            '' + '\n'.join([select_command, column_names_fexp, sql_rest]) + ';\n'
            '.END EXPORT;\n'
            '.LOGOFF;\n'
            )

        # Write script to file
        with open(script_filename, 'w') as f:
            f.writelines(self.fexp_script)

        # Write header to file
        with open(header_filename, 'w') as f:
            for col in column_info:
                f.write(col[0] + '\n')

    def to_dataframe(self, sql, file_name=None, delim='|', max_frac_digits_for_float=4, print_stdout=False):
        
        # Set filename to pass to to_csv()
        file_name_to_csv = file_name

        if file_name is None:
            file_name_to_csv = '.tdwrapper.to_dataframe.tmp'
        
        # Call to_csv() to export data to a file
        self.to_csv(sql, file_name_to_csv, delim=delim, max_frac_digits_for_float=max_frac_digits_for_float, print_stdout=print_stdout)

        # Read the final output file as DataFrame
        df = pd.read_csv(file_name_to_csv, sep=delim, na_values='?')
        
        # Delete final output file, if file name is not given by user
        if file_name is None:
            subprocess.call('if [ -f {} ]; then rm {}; fi;'.format(file_name_to_csv, file_name_to_csv), shell=True, executable=self.shell)

        # Return data frame
        return df

    def from_csv(self, file_name, table_name, column_names, column_types, column_sizes, print_stdout=False):
        self.fload_script = (
            'RUN ' + self.logon_string_file + ';\n'
            'drop table ' + self.log_database + '.' + self.userid + '_fload_err1;\n'
            'drop table ' + self.log_database + '.' + self.userid + '_fload_err2;\n'
            'drop table ' + table_name + ';\n'
            'create table ' + table_name + '(\n'
            '' + ',\n'.join('%s %s' % t for t in zip(column_names, column_types)) + '\n'
            ') no primary index;\n'
            'SET RECORD VARTEXT;\n'
            'DEFINE\n'
            '' + ',\n'.join('%s (VARCHAR(%s))' % t for t in zip(column_names, column_sizes)) + '\n'
            'FILE = ' + file_name + ';\n'
            'BEGIN LOADING ' + table_name + '\n'
            '    ERRORFILES ' + self.log_database + '.' + self.userid + '_fload_err1, ' + self.log_database + '.' + self.userid + '_fload_err2;\n'
            'INSERT INTO ' + table_name + ' VALUES (\n'
            '' + ',\n'.join(':%s' % t for t in column_names) + '\n'
            ');\n'
            '.END LOADING;\n'
            '.LOGOFF;\n'
        )

        # Execute FastLoad script
        p = subprocess.Popen('fastload', stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p.stdin.write(self.fload_script)
        
        if print_stdout:
            p.stdin.flush()
            # Now start grabbing output.
            while p.poll() is None:
                l = p.stdout.readline()
                print l
            print p.stdout.read()

        (self.fload_log, self.fload_err) = p.communicate()

        # Check if FastLoad run is successful
        if p.returncode is not 0:
            print self.fload_log
            print self.fload_err
            raise Exception('fload returns {}.'.format(p.returncode))

    def from_dataframe(self, df, table_name, file_name=None, print_stdout=False):
        
        # Set filename to pass to from_csv()
        file_name_from_csv = file_name

        if file_name is None:
            file_name_from_csv = '.tdwrapper.from_dataframe.tmp'

        # Write input DataFrame to csv file
        df.to_csv(file_name_from_csv, sep='|', na_rep='?', header=False, index=False)

        column_names = list(df.columns.values)
        
        column_dtypes = df.dtypes
        column_types = []
        column_sizes = []
        for i in xrange(len(column_dtypes)):
            dtype = column_dtypes[i].name
            if dtype == 'int32':
                column_types += ['INTEGER']
                column_sizes += [11]
            elif dtype == 'int64':
                column_types += ['BIGINT']
                column_sizes += [20]
            elif dtype == 'float64' or dtype == 'float32':
                column_types += ['FLOAT']
                column_sizes += [40]
            elif dtype == 'object':
                size = df.iloc[:, i].str.len().max() + 2
                if size > 64000:
                    raise Exception('Column {} exceeds the column length limit.'.format(column_names[i]))
                column_types += ['VARCHAR({})'.format(size)]
                column_sizes += [size]
            else:
                raise Exception('Unknown data type for column {}.'.format(column_names[i]))

        # Call to_csv() to export data to a file
        self.from_csv(
            file_name=file_name_from_csv, 
            table_name=table_name, 
            column_names=column_names,
            column_types=column_types,
            column_sizes=column_sizes,
            print_stdout=print_stdout
        )
        
        # Delete final output file, if file name is not given by user
        if file_name is None:
            subprocess.call('if [ -f {} ]; then rm {}; fi;'.format(file_name_from_csv, file_name_from_csv), shell=True, executable=self.shell)

    def clean_temp_files(self):
        subprocess.call('rm .tdwrapper.*', shell=True, executable=self.shell)