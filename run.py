import os
import datetime

# If the Operating System is Linux, Unix, or macOS, we can automatically generate a log of the terminal output
if os.name == 'posix':
    os.system("script -a ../{}_log.txt python main.py".format(datetime.datetime.now().strftime("%Y-%m-%d_%Hh%Mm%Ss")))
# Otherwise, just run the script without logging
else:
    os.system("python main.py")