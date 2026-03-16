# -*- coding: utf-8 -*-

from fetch_gridley import get_today_grid

rows, cols = get_today_grid()

print("\n====================")
print("GRIDLEY TEST RESULT")
print("====================\n")

print("ROWS\n")

for r in rows:
    print("-", r)

print("\nCOLS\n")

for c in cols:
    print("-", c)

print("\nCounts")
print("Rows:", len(rows))
print("Cols:", len(cols))