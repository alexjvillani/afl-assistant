from fetch_gridley import get_today_grid

rows, cols = get_today_grid()

print "\nROWS:"
for r in rows:
    print "-", r

print "\nCOLS:"
for c in cols:
    print "-", c