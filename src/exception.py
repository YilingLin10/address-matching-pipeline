class InvalidAddressTypeError(Exception):
    """Raised when the address type returned by usaddress is not 'Street Address'."""
    pass

class NormalizationError(Exception):
    """Custom exception for normalization failures."""
    pass


