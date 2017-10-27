import os
import stat
import subprocess
import shutil

class Executable:
    """Prepare and run 64 bit Linux executable with AWS Lambda
    
    Written by Brandon Owen - github.com/umeat
    """

    def __init__(self, source, no_move=False):
        """Prepare access and permissions for executable
        
        Source executable file is moved to /tmp/ directory and given execution 
        permissions - effectively 'chmod +x'
        
        Input:
            source      String containing relative location of executable file
            no_move     Whether or not to move executable to tmp space
            
        Self variables: 
            command     New location of executable, used to run executable
        """
        if no_move:
            self.command = source

        else:
            # Create copy of executable in /tmp/
            self.command = '/tmp/{}'.format(os.path.basename(source))

            shutil.copyfile(source, self.command)
        
        # Grant execution permissions
        st = os.stat(self.command)
        os.chmod(self.command, st.st_mode | stat.S_IEXEC)
        
    def run(self, args=''):
        # print('args is: {}'.format(args))
        """Run executable from shell with given arguments
        
        Input: 
            args        Optional, should be given a string to suffix a command 
                        line call to the executable, eg `-sw1 -sw2`
        
        Self variables: 
            stdout, stderr  Process standard communication channels
            returncode      Process exit status
        
        Process output variables are overwritten if run multiple times
        
        Additionally returns stdout
        """
        # Clear output variables from any previous execution
        self.stdout, self.stderr, self.returncode = None, None, None

        # Run executable from shell with arguments as string
        process = subprocess.Popen(
            '{} {}'.format(self.command, args), shell=True, 
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE)
            
        # Store stdout and stderr
        self.stdout, self.stderr = process.communicate()
        self.returncode = process.returncode
        
        # Return stdout
        return self.stdout
