import logging
import time
from collections import namedtuple
import traceback
from gpiozero import DigitalOutputDevice

_LOGGER = logging.getLogger(__name__)

RFProtocol = namedtuple('RFProtocol',
                      ['pulselength', 'msg_length', 'repeat_delay',
                       'sync_count', 'sync_delay',
                       'sync_high', 'sync_low',
                       'zero_high', 'zero_low',
                       'one_high', 'one_low', 'repeat_count'])

PROTOCOLS = (None,
             RFProtocol(20, 40, 10000, 1, 0, 5000, 1472, 17, 37, 35, 19, 6), # "home smart" shutter
             RFProtocol(300, 24, 0, 1, 0, 300, 9000, 1, 3, 3, 1, 4) # LED controller
             )

DEVICE_CODES = {}

class RFDevice:
    def __init__(self, gpio=17):
        """Initialize the RF device."""
        self.gpio = gpio
        self.controller = DigitalOutputDevice(gpio, active_high=True, initial_value=False)
        self.set_protocol(1)
        _LOGGER.debug("Using GPIO " + str(gpio))

    def set_protocol(self, tx_proto=1):
        self.tx_proto = tx_proto
        self.tx_repeat = PROTOCOLS[tx_proto].repeat_count
        self.tx_pulselength = PROTOCOLS[tx_proto].pulselength
        self.tx_repeat_delay = PROTOCOLS[tx_proto].repeat_delay
        self.tx_sync_count = PROTOCOLS[tx_proto].sync_count
        self.tx_sync_delay = PROTOCOLS[tx_proto].sync_delay
        self.tx_length = PROTOCOLS[tx_proto].msg_length

    def get_total_tx_length(self):
        return (self.tx_repeat * (
                    (self.tx_pulselength * self.tx_length) +
                    self.tx_sync_count * (self.tx_sync_delay + PROTOCOLS[self.tx_proto].sync_high + PROTOCOLS[self.tx_proto].sync_low)
                    )
                    + self.tx_repeat_delay * (self.tx_repeat - 1)
                )

    def cleanup(self):
        """Disable TX and clean up GPIO."""
        _LOGGER.debug("Cleanup")
        self.controller.close()

    def tx_code(self, code):
        """
        Send a decimal code.
        Optionally set protocol, pulselength and code length.
        When none given reset to default protocol, default pulselength and set code length to 40 bits.
        """

        rawcode = format(code, '#0{}b'.format(self.tx_length + 2))[2:]
        _LOGGER.debug("TX code: " + str(rawcode))
        return self._tx_bin(rawcode)

    def _tx_bin(self, rawcode):
        """Send a binary code, consider sync, delay and repeat parameters based on protocol."""
        #_LOGGER.debug("TX bin: {}" + str(rawcodes))
        sent = ""

        for _ in range(0, self.tx_repeat):
            for x in list(range(self.tx_sync_count)):
                if self._tx_sync():
                    sent += "$"
                else:
                    return False
            if (self.tx_sync_delay > 0):
                if (self._tx_delay(self.tx_sync_delay)):
                    sent += "&"
                else:
                    return False
            for byte in range(0, self.tx_length):
                if rawcode[byte] == '0':
                    if self._tx_l0():
                        sent += "0"
                    else:
                        return False
                else:
                    if self._tx_l1():
                        sent += "1"
                    else:
                        return False
            if (self.tx_repeat_delay > 0):
                if self._tx_delay(self.tx_repeat_delay):
                    sent += '|'
                else:
                    return False
        _LOGGER.debug("sent: {}".format(sent))
        return True

    def _tx_l0(self):
        """Send a '0' bit."""
        if not 0 < self.tx_proto < len(PROTOCOLS):
            _LOGGER.error("Unknown TX protocol")
            return False
        return self._tx_waveform(PROTOCOLS[self.tx_proto].zero_high,
                                PROTOCOLS[self.tx_proto].zero_low)

    def _tx_l1(self):
        """Send a '1' bit."""
        if not 0 < self.tx_proto < len(PROTOCOLS):
            _LOGGER.error("Unknown TX protocol")
            return False
        return self._tx_waveform(PROTOCOLS[self.tx_proto].one_high,
                                PROTOCOLS[self.tx_proto].one_low)

    def _tx_sync(self):
        """Send a sync."""
        if not 0 < self.tx_proto < len(PROTOCOLS):
            _LOGGER.error("Unknown TX protocol")
            return False
        return self._tx_waveform_irregular(PROTOCOLS[self.tx_proto].sync_high,
                                PROTOCOLS[self.tx_proto].sync_low)

    def _tx_delay(self, delay):
        """Wait between repeats."""
        self.controller.off()
        time.sleep((delay) / 1000000.0)
        return True

    def _tx_waveform(self, highpulses, lowpulses):
        """Send basic waveform."""
        self.controller.on()
        time.sleep((highpulses * self.tx_pulselength) / 1000000.0)
        self.controller.off()
        time.sleep((lowpulses * self.tx_pulselength) / 1000000.0)
        return True

    def _tx_waveform_irregular(self, highpulses, lowpulses):
        """Send waveform without using regular pulse length."""
        self.controller.on()
        time.sleep((highpulses) / 1000000.0)
        self.controller.off()
        time.sleep((lowpulses) / 1000000.0)
        return True

    def control(self, command):
        try:
            _LOGGER.info(command)
            self.tx_code(command)
            time.sleep((self.get_total_tx_length() / 1000000.0))
            return True
        except Exception as e:
            _LOGGER.error(f"exception while sending command: {e}")
            _LOGGER.error(traceback.format_exc())
            return False
