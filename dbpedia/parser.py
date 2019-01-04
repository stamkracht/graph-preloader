import codecs
import sys

import rdflib.plugins.parsers.ntriples as rdflib_nt


class NTriplesParser(rdflib_nt.NTriplesParser):
    """
    Extends RDFlib's NTriples parser with:
    - more flexible error handling;
    - parsing part of an NTriples file;
    - updating a progress bar;

    Last updated for rdflib 5.0.0.dev0
    """

    def __init__(self, sink=None, update_progress=None):
        super().__init__(sink=sink)
        self.update_progress = update_progress
        self.file = None
        self.buffer = None
        self.line = None

    def parse(self, in_file, left=0, right=None):
        """
        Parse in_file as an N-Triples file.
        Start reading at `left` and stop reading at `right` (byte/character offsets).
        """
        if not hasattr(in_file, 'read'):
            raise rdflib_nt.ParseError("Item to parse must be a file-like object.")

        cursor = in_file.seek(left)
        progress_buffer = 0
        # since N-Triples 1.1 files can and should be utf-8 encoded
        utf8_stream = codecs.getreader('utf-8')(in_file)
        self.file = utf8_stream
        self.buffer = ''

        while True:
            line = self.readline()
            self.line = line
            new_cursor = in_file.tell()
            if self.update_progress:
                progress = new_cursor - cursor
                if right:
                    progress = min(new_cursor, right) - cursor

                progress_buffer += progress
                if progress_buffer > 1024 * 1024:
                    self.update_progress(progress_buffer)
                    progress_buffer = 0

            if self.line is None:
                break
            elif right and new_cursor >= right:
                break
            elif new_cursor < left:
                raise rdflib_nt.ParseError(
                    f'File cursor at {new_cursor} is lower '
                    f'than its starting point at {left}'
                )
            cursor = new_cursor

            try:
                self.parseline()
            except rdflib_nt.ParseError:
                print(
                    f'Parser choked on {repr(self.line)} in line: {line}',
                    file=sys.stderr
                )

        return self.sink
