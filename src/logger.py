import logging

# Create and export a logger
formatter = logging.Formatter(
    "%(asctime)s.%(msecs)03d | %(levelname)-7s | %(filename)s:%(lineno)-3s | %(message)s",
    "%Y-%m-%d %H:%M:%S",
)

log = logging.getLogger()
# TODO: Would be nicer to have a CLI variable that sets this rather than hardcoding
log.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
log.addHandler(console_handler)
