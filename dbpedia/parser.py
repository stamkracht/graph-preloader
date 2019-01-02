import codecs
import sys

import rdflib.plugins.parsers.ntriples as rdflib_nt


class NTriplesParser(rdflib_nt.NTriplesParser):
    """
    Extends RDFlib's NTriples parser with more flexible error handling.

    Last updated for rdflib 5.0.0.dev0
    """

    def parse(self, f):
        """Parse f as an N-Triples file."""
        if not hasattr(f, 'read'):
            raise rdflib_nt.ParseError("Item to parse must be a file-like object.")

        # since N-Triples 1.1 files can and should be utf-8 encoded
        f = codecs.getreader('utf-8')(f)

        self.file = f
        self.buffer = ''
        while True:
            line = self.readline()
            if line is None:
                break
            try:
                self.parseline()
            except rdflib_nt.ParseError:
                raise print(f'Invalid line: {line}', file=sys.stderr)
        return self.sink
