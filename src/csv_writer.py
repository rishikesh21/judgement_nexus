import os
import csv


class CaseCSVWriter:
    def __init__(self, path, header):
        self.path = path
        self.header = header
        self._ensure_header()

    def _ensure_header(self):
        if not os.path.exists(self.path):
            with open(self.path, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(self.header)

    def append_rows(self, rows):
        if not rows:
            return
        with open(self.path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerows(rows)