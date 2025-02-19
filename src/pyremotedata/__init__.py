import datetime
import logging
import os

# Adds ability to remove end-of-line in log messages.
# Courtesy of: https://stackoverflow.com/a/65235302/19104786
ESC_EOL = '[!n]'
CLEAR_LINE = f'\r{" " * 100}\r'

class MStreamHandler(logging.StreamHandler):
  """Handler that controls the writing of the newline character"""

  def emit(self, record) -> None:
    msg = str(record.msg)
    if msg.endswith(ESC_EOL):
        record.msg = msg.replace(ESC_EOL, '')
        oterm = self.terminator
        ofmt = self.formatter
        self.terminator = ''
        self.formatter = logging.Formatter('%(message)s')
        retval = super().emit(record)
        self.terminator = oterm
        self.formatter = ofmt
        return retval
    else:
       return super().emit(record)
    
class MFileHandler(logging.StreamHandler):
  """Handler that controls the writing of the newline character"""

  def emit(self, record) -> None:
    if record.msg.endswith(ESC_EOL):
        record.msg = record.msg.replace(ESC_EOL, '')
    if record.msg.startswith(CLEAR_LINE):
       record.msg = record.msg.replace(CLEAR_LINE, '')
    return super().emit(record)    

# Configure logging.
# `export DEBUG=1` to see debug output.
# `mkdir logs` to write to files too.
# Create loggers with `import logging; logger = logging.getLogger(__name__)`

module_logger = logging.getLogger(__name__)
module_logger.setLevel(logging.INFO if not os.environ.get('DEBUG') else logging.DEBUG)
main_logger = logging.getLogger('__main__')
main_logger.setLevel(logging.INFO if not os.environ.get('DEBUG') else logging.DEBUG)

console_handler = MStreamHandler()
console_handler.setLevel(logging.DEBUG)

try:
    import colorlog  # `pip install colorlog` for extra goodies.

    stream_formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s %(levelname)-6s %(cyan)s%(name)-10s %(white)s%(message)s',
        "%H:%M:%S",
        log_colors={
            'DEBUG': 'blue',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
            'EXCEPTION': 'bold_red',
        })
except ImportError:
    stream_formatter = logging.Formatter('%(levelname)s %(name)s %(message)s')
console_handler.setFormatter(stream_formatter)

package_timestamp = datetime.datetime.now().strftime('%Y%m%dT%H%M%S')
if os.path.isdir('logs'):
    file_handler = MFileHandler(os.path.join('logs', f'{__name__}_{package_timestamp}.log'), mode='a')
    file_handler.setLevel(logging.INFO if not os.environ.get('DEBUG') else logging.DEBUG)
    file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s'))
    module_logger.addHandler(file_handler)
    main_logger.addHandler(file_handler)

module_logger.addHandler(console_handler)
main_logger.addHandler(console_handler)