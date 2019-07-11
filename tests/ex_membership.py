import datalink
import logging
import sys

logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                    level=logging.DEBUG,
                    stream=sys.stdout)
log = logging.getLogger(__name__)

# logging.getLogger('datalink').propagate = False
# datalink.test_output()

log.info(dir(datalink))

