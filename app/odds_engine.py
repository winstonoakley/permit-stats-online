import os
import sqlite3
import datetime as dt
from dataclasses import dataclass
from typing import List, Dict, Any

cur = None
corezoneid = None

@dataclass
class Choice:
    zone: str
    month: int
    day: int
    group_size: int


# Estimating odds of a single choiceset
# Convert zone name to zoneID
def findzoneid(z):
    cur.execute('''SELECT zone.zone_id
            FROM zone
            WHERE zone.zonename = ?''', (z,))
    r = cur.fetchall()
    return r[0][0]

# Convert date string to dateID
def finddateid(d):
    cur.execute('''SELECT date.date_id
                FROM date
                WHERE date.datestr = ?''', (d,))
    r = cur.fetchall()
    return r[0][0]

# Building the Core zone group size scaling dictionary
coregs = {1:4.64, 2:1.41, 3:1.15, 4:1.18, 5:1.07, 6:1.2, 7:1.03}

# Lottery season bounds (month, day) and the neighbour-smoothing kernel used by
# the calibrated predictor (see predict_first_choice at the bottom of this file).
SEASON_START_MD = (5, 15)   # May 15
SEASON_END_MD = (10, 31)    # Oct 31
NEIGHBOUR_WEEKS = 1         # same-weekday dates this many weeks either side
SMOOTH_WEIGHTS = {0: 1.0, 1: 0.5}   # weight by |week offset|; chosen by backtest

# Find relevant records
# Fetch exact match
def checkexact(cnum, z1, d1, g1, z2, d2, g2, z3, d3, g3):
    if cnum == 1:
        if z1 == corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.choicenum = 1
                        AND wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.groupsize1 = ?''', (corezoneid, d1, g1))
            r = cur.fetchall()

        else:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.choicenum = 1
                        AND wins.zoneid1 = ?
                        AND wins.dateid1 = ?''', (z1, d1))
            r = cur.fetchall()

    elif cnum == 2:
        if z1 == corezoneid & z2 == corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.groupsize1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.groupsize2 = ?
                        AND wins.choicenum = 2''', (corezoneid, d1, g1, corezoneid, d2, g2))
            r = cur.fetchall()
        elif z1 != corezoneid & z2 == corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.groupsize2 = ?
                        AND wins.choicenum = 2''', (z1, d1, corezoneid, d2, g2))
            r = cur.fetchall()
        elif z1 == corezoneid & z2 != corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.groupsize1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.choicenum = 2''', (corezoneid, d1, g1, z2, d2))
            r = cur.fetchall()
        elif z1 != corezoneid & z2 != corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.choicenum = 2''', (z1, d1, z2, d2))
            r = cur.fetchall()

    elif cnum == 3:
        if z1 == corezoneid & z2 == corezoneid & z3 == corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.groupsize1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.groupsize2 = ?
                        AND wins.zoneid3 = ?
                        AND wins.dateid3 = ?
                        AND wins.groupsize3 = ?
                        AND wins.choicenum = 3''', (corezoneid, d1, g1, corezoneid, d2, g2, corezoneid, d3, g3))
            r = cur.fetchall()
        elif z1 != corezoneid & z2 == corezoneid & z3 == corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.groupsize2 = ?
                        AND wins.zoneid3 = ?
                        AND wins.dateid3 = ?
                        AND wins.groupsize3 = ?
                        AND wins.choicenum = 3''', (z1, d1, corezoneid, d2, g2, corezoneid, d3, g3))
            r = cur.fetchall()
        elif z1 == corezoneid & z2 != corezoneid & z3 == corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.groupsize1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.zoneid3 = ?
                        AND wins.dateid3 = ?
                        AND wins.groupsize3 = ?
                        AND wins.choicenum = 3''', (corezoneid, d1, g1, z2, d2, corezoneid, d3, g3))
            r = cur.fetchall()
        elif z1 == corezoneid & z2 == corezoneid & z3 != corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.groupsize1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.groupsize2 = ?
                        AND wins.zoneid3 = ?
                        AND wins.dateid3 = ?
                        AND wins.choicenum = 3''', (corezoneid, d1, g1, corezoneid, d2, g2, z3, d3))
            r = cur.fetchall()
        elif z1 != corezoneid & z2 != corezoneid & z3 == corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.zoneid3 = ?
                        AND wins.dateid3 = ?
                        AND wins.groupsize3 = ?
                        AND wins.choicenum = 3''', (z1, d1, z2, d2, corezoneid, d3, g3))
            r = cur.fetchall()
        elif z1 != corezoneid & z2 == corezoneid & z3 != corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.groupsize2 = ?
                        AND wins.zoneid3 = ?
                        AND wins.dateid3 = ?
                        AND wins.choicenum = 3''', (z1, d1, corezoneid, d2, g2, z3, d3))
            r = cur.fetchall()
        elif z1 == corezoneid & z2 != corezoneid & z3 != corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.groupsize1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.zoneid3 = ?
                        AND wins.dateid3 = ?
                        AND wins.choicenum = 3''', (corezoneid, d1, g1, z2, d2, z3, d3))
            r = cur.fetchall()
        elif z1 != corezoneid & z2 != corezoneid & z3 != corezoneid:
            cur.execute('''SELECT wins.avgodds
                        FROM wins
                        WHERE wins.zoneid1 = ?
                        AND wins.dateid1 = ?
                        AND wins.zoneid2 = ?
                        AND wins.dateid2 = ?
                        AND wins.zoneid3 = ?
                        AND wins.dateid3 = ?
                        AND wins.choicenum = 3''', (z1, d1, z2, d2, z3, d3))
            r = cur.fetchall()
    return r

# 1st Choice
def fetchCore1(d, g):
    # Gather the avg odds for group sizes in the given dateID
    cur.execute('''SELECT wins.groupsize1, wins.avgodds
                FROM wins
                WHERE wins.choicenum = 1
                AND wins.zoneid1 = ?
                AND wins.dateid1 = ?''', (corezoneid, d))
    r = cur.fetchall()

    return r

# 2nd Choice
def fetchCore2(z1, d1, g1, d2, g2, c1odds):
    r = checkexact(2, z1, d1, g1, corezoneid, d2, g2, 0, 0, 0)

    # Check if C1/C2 set was chosen by prior applicants
    if len(r) == 1: # C1/C2 was chosen previously
        c2odds = r[0][0]
        return c2odds

    else: # C1/C2 was not chosen previously
        # Quering the odds for C2 as 1st choice
        C2aC1odds = coreodds1(d2, g2)
        c2odds = EstC2Odds(C2aC1odds, c1odds)
        return c2odds

# 3rd Choice
def fetchCore3(z1, d1, g1, z2, d2, g2, d3, g3, c1odds, c2odds):
    # Check if C1/C2 set was chosen by prior applicants
    r = checkexact(3, z1, d1, g1, z2, d2, g2, corezoneid, d3, g3)

    if len(r) == 1: # C1/C2/C3 was chosen previously
        c3odds = r[0][0]

    else: # C1/C2/C3 was not chosen previously
        # Quering the odds for C3 as 1st choice
        C3aC1odds = coreodds1(d3, g3)
        c3odds = EstC3Odds(C3aC1odds, c2odds, c1odds)

    return c3odds

# Estimating Choice 2 odds
def EstC2Odds(C2aC1odds, c1odds):
    c2moe = 0.01 # Range of C2 error
    c1moe = 0.02 # Range of C1 error
    k = 1
    c2match = False
    firstresult = True

    while k < 11 and c2match == False:
        cur.execute('''select c1odds, c2a1odds, wins.avgodds as c2odds
                            from (select zc1, dc1, wins.avgodds as c1odds, zc2a1, dc2a1, c2a1odds
                            from (select zc2a1, dc2a1, c2a1odds, wins.zoneid1 as zc1, wins.dateid1 as dc1
                            from (select wins.zoneid1 as zc2a1, wins.dateid1 as dc2a1, wins.avgodds as c2a1odds
                            from wins
                            where wins.choicenum = 1
                            and wins.avgodds < ?
                            and wins.avgodds > ?) join wins
                            where choicenum = 2
                            and wins.zoneid2 = zc2a1
                            and wins.dateid2 = dc2a1) join wins
                            where wins.choicenum = 1
                            and wins.zoneid1 = zc1
                            and wins.dateid1 = dc1
                            and wins.avgodds < ?
                            and wins.avgodds > ?) join wins
                            where choicenum = 2
                            and wins.zoneid1 = zc1
                            and wins.dateid1 = dc1
                            and wins.zoneid2 = zc2a1
                            and wins.dateid2 = dc2a1''',
                            (C2aC1odds + c2moe, C2aC1odds - c2moe,
                             c1odds + c1moe, c1odds - c1moe))
        r2 = cur.fetchall()

        if len(r2) > 0 and firstresult == True:
            lowestr2 = r2
            firstresult = False

        # Check if there are any results
        if len(r2) == 1: # Single result
            c2odds = r2[0][2]
            c2match = True
            c2odds
        elif len(r2) > 1 and len(r2) <= 30: # Multiple results; average the results
            lowodds = 0
            lowerr = 0.01
            err1 = True
            m = 0
            while m < len(r2) and lowerr != 0:
                error = round(((c1odds-r2[m][0])*1000)**2 + ((C2aC1odds-r2[m][1])*1000)**2,0)
                if err1 == True:
                    lowodds = r2[m][2]
                    lowerr = error
                    err1 = False
                elif error < lowerr:
                    lowodds = r2[m][2]
                    lowerr = error
                m = m + 1
            c2match = True
            c2odds = lowodds
        elif len(r2) > 30: # Too many results
            c1moe = c1moe / 2
            c2moe = c2moe / 2
            if k == 1:
                lowestr2 = r2
            if len(r2) < len(lowestr2):
                lowestr2 = r2
            k = k + 1
        elif len(r2) == 0 and k == 11: # Widening didn't work
            print('No matches found for C2aC1odds')
        else: # No results; widen range
            c1moe = c1moe * 1.5
            c2moe = c2moe * 1.5
            k = k + 1

    if c2match == False:
        sumr2 = 0
        for j in lowestr2:
            sumr2 = sumr2 + j[2]
        c2odds = sumr2 / len(lowestr2)

    return c2odds

# Estimating Choice 3 odds
def EstC3Odds(C3aC1odds, c2odds, c1odds):
    # Find the odds of a C1/C2/C3 set with similar odds to C3asC1, C2, & C1
    c3moe = 0.01 # Range of C3 error
    c2moe = 0.01 # Range of C2 error
    c1moe = 0.01 # Range of C1 error
    k = 1
    c3match = False
    firstresult = True

    while k < 11 and c3match == False:
        cur.execute('''select c3odds, c3a1odds, c2odds, c1odds
                    from (select c3odds, zc1a, dc1a, c3a1odds, c2odds
                    from (select wins.zoneid1 as zc1a, wins.dateid1 as dc1a, wins.zoneid2 as zc2a, wins.dateid2 as dc2a, wins.avgodds as c3odds, c3a1odds
                    from (SELECT wins.zoneid1 as zc3a1, wins.dateid1 as dc3a1, wins.avgodds as c3a1odds
                    from wins
                    where wins.choicenum = 1
                    and wins.avgodds < ?
                    and wins.avgodds > ?) join wins
                    where wins.zoneid3 = zc3a1
                    and wins.dateid3 = dc3a1) join
                    (select wins.zoneid2 as zc2, wins.dateid2 as dc2, wins.avgodds as c2odds, wins.zoneid1 as zc1b, wins.dateid1 as dc1b
                    from wins
                    where wins.choicenum = 2
                    and wins.avgodds < ?
                    and wins.avgodds > ?)
                    where zc2 = zc2a
                    and dc2 = dc2a
                    and zc1b = zc1a
                    and dc1b = dc1a) join
                    (select wins.zoneid1 as zc1, wins.dateid1 as dc1, wins.avgodds as c1odds
                    from wins
                    where wins.choicenum = 1
                    and wins.avgodds < ?
                    and wins.avgodds > ?)
                    where zc1 = zc1a
                    and dc1 = dc1a''',
                    (C3aC1odds + c3moe, C3aC1odds - c3moe,
                     c2odds + c2moe, c2odds - c2moe,
                     c1odds + c1moe, c1odds - c1moe))
        r3 = cur.fetchall()
        if len(r3) > 0 and firstresult == True:
            lowestr3 = r3
            firstresult = False

        # Check if there are any results
        if len(r3) == 1: # Single result
            c3odds = r3[0][0]
            c3match = True
        elif len(r3) > 1 and len(r3) <= 30: # Multiple results; average the results
            lowodds = 0
            lowerr = 0.01
            err1 = True
            m = 0
            while m < len(r3) and lowerr != 0:
                error = round(((c1odds-r3[m][3])*1000)**2 + round((c2odds-r3[m][2])*1000)**2 + round((C3aC1odds-r3[m][1])*1000)**2,0)
                if err1 == True:
                    lowodds = r3[m][0]
                    lowerr = error
                    err1 = False
                elif error < lowerr:
                    lowodds = r3[m][0]
                    lowerr = error
                m = m + 1
            c3match = True
            c3odds = lowodds
        elif len(r3) > 30: # Too many results
            c1moe = c1moe / 2
            c2moe = c2moe / 2
            c3moe = c3moe / 2
            if k == 1:
                lowestr3 = r3
            if len(r3) < len(lowestr3):
                lowestr3 = r3
            k = k + 1
        else: # No results; widen range
            c1moe = c1moe * 1.5
            c2moe = c2moe * 1.5
            c3moe = c3moe * 1.5
            k = k + 1

    if c3match == False:
        sumr3 = 0
        for j in lowestr3:
            sumr3 = sumr3 + j[0]
        c3odds = sumr3 / len(lowestr3)

    return c3odds

# Find similar odds for Choice 1 Core zone
def coreodds1(d, g):
    # Gather the avg odds for group sizes in the given dateID
    r = fetchCore1(d, g)

    exactmatch = False
    lowergs = 0
    lowerodds = 0
    highergs = 9
    higherodds = 0

    for gs in r: # Iterate through results
        if gs[0] == g: # A result matches the chosen group size
            return gs[1] # Return the average odds
        elif gs[0] < g and gs[0] > lowergs:
            lowergs = gs[0]
            lowerodds = gs[1]
        elif gs[0] > g and gs [0] < highergs:
            highergs = gs[0]
            higherodds = gs[1]

    # No exact match for the group size; use linear extrapolation
    if lowergs != 0 and highergs != 9: # There are observed group sizes above and below the chosen group size
        incr = (lowerodds - higherodds) / (highergs - lowergs)
        extrpodds = higherodds + incr * (highergs - g)

    elif lowergs == 0 and highergs != 9: # There is an observed group size above the chosen size but not below
        gsc = highergs
        estodds = higherodds

        while gsc > g:
            estodds = estodds * coregs[gsc-1]
            gsc = gsc - 1
        if estodds > 1: # Limiting factored odds to 100%
            estodds = 1
        extrpodds = estodds

    elif lowergs !=0 and highergs == 9: # There is an observed group size below the chosen size but not above
        gsc = lowergs
        estodds = lowerodds

        while gsc < g:
            estodds = estodds / coregs[gsc]
            gsc = gsc + 1
        if estodds > 1: # Limiting factored odds to 100%
            estodds = 1
        extrpodds = estodds
    try:
        return extrpodds
    except:
        print('Error in estimating group size odds')

def find_comp_date(permit_date, data_year):
    def adjust_base_date(base_date, target_month):
        while base_date.month != target_month:
            if base_date.month < target_month:
                base_date += dt.timedelta(1)
            else:
                base_date -= dt.timedelta(1)
        return base_date

    def find_closest_weekday(base_date, target_weekday, search_direction):
        day = base_date
        count = 0
        while day.weekday() != target_weekday:
            day += dt.timedelta(search_direction)
            count += 1
        return day, count

    diy = permit_date.timetuple().tm_yday
    base_date = dt.date(data_year, 1, 1) + dt.timedelta(diy - 1)
    base_date = adjust_base_date(base_date, permit_date.month)

    down_day, down_count = find_closest_weekday(base_date, permit_date.weekday(), -1)
    up_day, up_count = find_closest_weekday(base_date, permit_date.weekday(), 1)

    result = down_day if down_count <= up_count else up_day

    season_end = dt.date(data_year, 10, 31)
    if result > season_end:
        result, _ = find_closest_weekday(season_end, permit_date.weekday(), -1)

    return result

# Find the number of weekday within the month
def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + dt.timedelta(n)

def weekdayinmonth(targetdate):
    lastdayinmonth = calendar.monthrange(targetdate.year, targetdate.month)[1]
    start_date = dt.date(targetdate.year, targetdate.month, 1)
    end_date = dt.date(targetdate.year, targetdate.month, lastdayinmonth)
    suffixdict = {1:'st',2:'nd',3:'rd',4:'th',5:'th'}
    wdcount = 0

    for idate in (start_date + dt.timedelta(n) for n in range(lastdayinmonth)):
        if idate.weekday() == targetdate.weekday():
            wdcount = wdcount + 1
            if idate == targetdate:
                targetweekdayinmo = wdcount

    return [targetweekdayinmo, suffixdict[targetweekdayinmo],wdcount]

# Convert date number to date str
def dateconv(datenum):
    dstr = str(datenum)
    if len(dstr) == 1:
        dstr = '0' + dstr
    return dstr

def estimate_season_odds(
    zone: str,
    group_size: int,
    permit_year: int,
    data_years: List[int],
    db_dir: str,
) -> Dict[str, Any]:
    """
    Compute first-choice odds for a single zone/group size for every date in
    the lottery season (May 15 - Oct 31 of permit_year), across data_years.

    Opens each year's database once and iterates over all season dates,
    caching by comparable date (many permit dates share one comp date).

    Returns:
        {
          "zone": ...,
          "group_size": ...,
          "permit_year": ...,
          "data_years": [...],
          "odds_by_date": {
            "YYYY-MM-DD": {year: float or None, ...},
            ...
          }
        }
    """
    global cur, corezoneid

    season_start = dt.date(permit_year, 5, 15)
    season_end = dt.date(permit_year, 10, 31)
    season_dates = [
        season_start + dt.timedelta(n)
        for n in range((season_end - season_start).days + 1)
    ]

    odds_by_date: Dict[str, Dict[int, Any]] = {
        d.isoformat(): {} for d in season_dates
    }
    # Per permit date: neighbour-smoothed odds for each year that has data.
    smoothed_by_date: Dict[str, List[float]] = {
        d.isoformat(): [] for d in season_dates
    }

    for dyear in data_years:
        db_path = os.path.join(db_dir, f"odds_{dyear}.db")
        if not os.path.exists(db_path):
            for d in season_dates:
                odds_by_date[d.isoformat()][dyear] = None
            continue

        conn = sqlite3.connect(db_path)
        cur = conn.cursor()

        try:
            cur.execute("SELECT zone_id FROM zone WHERE zonename = 'Core'")
            row = cur.fetchone()
            corezoneid = row[0] if row else None

            try:
                zid = findzoneid(zone)
            except Exception:
                for d in season_dates:
                    odds_by_date[d.isoformat()][dyear] = None
                continue

            season_start_y = dt.date(dyear, *SEASON_START_MD)
            season_end_y = dt.date(dyear, *SEASON_END_MD)
            # Actual date in dyear -> (odds, obs) or None. Many permit dates
            # share a comparable date, and neighbours overlap, so cache.
            odds_cache: Dict[dt.date, Any] = {}

            def odds_on(date_obj):
                if date_obj < season_start_y or date_obj > season_end_y:
                    return None
                if date_obj not in odds_cache:
                    try:
                        odds_cache[date_obj] = _c1_odds_on_date(zid, group_size, date_obj)
                    except Exception:
                        odds_cache[date_obj] = None
                return odds_cache[date_obj]

            for d in season_dates:
                iso = d.isoformat()
                try:
                    comp_date = find_comp_date(d, dyear)
                except Exception:
                    odds_by_date[iso][dyear] = None
                    continue

                r = odds_on(comp_date)
                odds_by_date[iso][dyear] = r[0] if r is not None else None
                if r is None:
                    continue

                rec = {"odds": r[0], "obs": r[1], "neighbours": []}
                for k in range(-NEIGHBOUR_WEEKS, NEIGHBOUR_WEEKS + 1):
                    if k == 0:
                        continue
                    nr = odds_on(comp_date + dt.timedelta(days=7 * k))
                    if nr is not None:
                        rec["neighbours"].append((k, nr[0], nr[1]))
                smoothed_by_date[iso].append(smooth_year(rec))
        finally:
            conn.close()

    predicted_by_date: Dict[str, Any] = {
        iso: (max(0.0, min(1.0, sum(vals) / len(vals))) if vals else None)
        for iso, vals in smoothed_by_date.items()
    }

    return {
        "zone": zone,
        "group_size": group_size,
        "permit_year": permit_year,
        "data_years": data_years,
        "odds_by_date": odds_by_date,
        "predicted_by_date": predicted_by_date,
    }


def estimate_odds_for_choice_set(
    permit_year: int,
    choices: List[Choice],
    data_years: List[int],
    db_dir: str,
) -> Dict[str, Any]:
    """
    Simpler, robust estimator:

    - Treats each provided choice independently as a first choice (C1 only).
    - For each choice and each data_year:
        * Uses find_comp_date(permit_year, data_year) for comparable date.
        * Looks up odds via:
            - coreodds1(...) if zone is the core zone that year.
            - the single (zone, date) row otherwise.
    - Adds a calibrated prediction for permit_year from the same history
      (see predict_first_choice).
    - Returns:
        {
          "years": [...],
          "choices": [
            {
              "index": 1..N,
              "zone": ...,
              "month": ...,
              "day": ...,
              "group_size": ...,
              "display_date": "MM-DD-YYYY" (permit_year),
              "odds_by_year": {year: float or None},
              "obs_by_year": {year: int or None},
              "comp_dates_by_year": {year: "MM-DD-YYYY" or None},
              "predicted": float or None
            },
            ...
          ]
        }
    """
    result_choices: List[Dict[str, Any]] = []

    for idx, c in enumerate(choices, start=1):
        # Build a display date from the permit year and the choice's month/day
        display_date = f"{c.month:02d}-{c.day:02d}-{permit_year}"

        # If the choice is clearly invalid, fill zeros
        if (not c.zone) or (c.month < 1 or c.month > 12) or (c.day < 1 or c.day > 31):
            result_choices.append(
                {
                    "index": idx,
                    "zone": c.zone,
                    "month": c.month,
                    "day": c.day,
                    "group_size": c.group_size,
                    "display_date": display_date,
                    "odds_by_year": {dyear: 0.0 for dyear in data_years},
                    "obs_by_year": {dyear: None for dyear in data_years},
                    "comp_dates_by_year": {dyear: None for dyear in data_years},
                    "predicted": None,
                }
            )
            continue

        try:
            permit_date = dt.date(permit_year, c.month, c.day)
        except ValueError:
            permit_date = None

        if permit_date is None:
            history = {
                dyear: {"comp_date": None, "odds": None, "obs": None, "neighbours": []}
                for dyear in data_years
            }
        else:
            history = lookup_first_choice_history(
                c.zone, c.group_size, permit_date, data_years, db_dir
            )

        odds_by_year: Dict[int, Any] = {}
        obs_by_year: Dict[int, Any] = {}
        comp_dates_by_year: Dict[int, Any] = {}
        for dyear in data_years:
            rec = history[dyear]
            db_missing = not os.path.exists(os.path.join(db_dir, f"odds_{dyear}.db"))
            # A missing database file reports 0.0 (historical behaviour);
            # a missing row reports None so the page shows "Data Unavailable".
            odds_by_year[dyear] = 0.0 if db_missing else rec["odds"]
            obs_by_year[dyear] = rec["obs"]
            comp_dates_by_year[dyear] = rec["comp_date"]

        result_choices.append(
            {
                "index": idx,
                "zone": c.zone,
                "month": c.month,
                "day": c.day,
                "group_size": c.group_size,
                "display_date": display_date,
                "odds_by_year": odds_by_year,
                "obs_by_year": obs_by_year,
                "comp_dates_by_year": comp_dates_by_year,
                "predicted": predict_first_choice(history),
            }
        )

    return {
        "years": data_years,
        "choices": result_choices,
    }


# ---------------------------------------------------------------------------
# First-choice history lookup and calibrated prediction
# ---------------------------------------------------------------------------
#
# The site used to extrapolate a logit-linear trend through the last three
# years in the browser. Backtesting showed that over-shoots badly whenever a
# year is backed by 0-2 applications (which is every high-odds date), so the
# prediction now lives here, is shared with backtest.py, and works as follows:
#
#   1. For each data year, look up the comparable date's odds and also the
#      odds on the same weekday one week either side (same zone, same group
#      size, same year). Nearby same-weekday dates share the same demand
#      regime, so averaging them damps the noise of thin dates.
#   2. Kernel-average those per year, in probability space (never logit
#      space, where an imputed 100% would be infinite).
#   3. Take the plain mean across years. No trend is fitted.
#
# Backtest (1000 random choices, predicting 2025 from 2022-2024 and 2026 from
# 2022-2025): Brier 0.0326 -> 0.0103 and 0.0260 -> 0.0083; the 40%+ bucket
# went from +9 pp over-confident to within 0.2 pp. Constants live near the
# top of the file (SEASON_*_MD, NEIGHBOUR_WEEKS, SMOOTH_WEIGHTS).


def _c1_odds_on_date(zid, gs, date_obj):
    """
    First-choice odds for zone `zid`, group size `gs`, on an actual calendar
    date in the currently open database. Returns (odds, obs) or None.
    """
    date_str = date_obj.strftime('%m-%d-%Y')
    cur.execute("SELECT date_id FROM date WHERE datestr = ?", (date_str,))
    row = cur.fetchone()
    if not row:
        return None
    did = row[0]

    if zid == corezoneid:
        cur.execute('''SELECT groupsize1, avgodds, obs
                       FROM wins
                       WHERE choicenum = 1 AND zoneid1 = ? AND dateid1 = ?''', (zid, did))
        rows = cur.fetchall()
        if not rows:
            return None
        for g, p, o in rows:
            if g == gs:
                return (float(p), int(o))
        v = coreodds1(did, gs)  # interpolate / scale between observed sizes
        if v is None:
            return None
        return (float(v), min(int(o) for _, _, o in rows))

    cur.execute('''SELECT avgodds, obs
                   FROM wins
                   WHERE choicenum = 1 AND zoneid1 = ? AND dateid1 = ?''', (zid, did))
    row = cur.fetchone()
    if not row:
        return None
    return (float(row[0]), int(row[1]))


def lookup_first_choice_history(
    zone: str,
    group_size: int,
    permit_date: dt.date,
    data_years: List[int],
    db_dir: str,
    neighbour_weeks: int = NEIGHBOUR_WEEKS,
) -> Dict[int, Dict[str, Any]]:
    """
    Per data year: the comparable date's odds/obs plus the odds on the same
    weekday up to `neighbour_weeks` weeks either side.

    Returns:
        {year: {"comp_date": "MM-DD-YYYY" or None,
                "odds": float or None,
                "obs": int or None,
                "neighbours": [(week_offset, odds, obs), ...]}}
    """
    global cur, corezoneid

    out: Dict[int, Dict[str, Any]] = {}
    for dyear in data_years:
        rec: Dict[str, Any] = {"comp_date": None, "odds": None, "obs": None, "neighbours": []}
        out[dyear] = rec

        db_path = os.path.join(db_dir, f"odds_{dyear}.db")
        if not os.path.exists(db_path):
            continue

        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        try:
            cur.execute("SELECT zone_id FROM zone WHERE zonename = 'Core'")
            row = cur.fetchone()
            corezoneid = row[0] if row else None
            try:
                zid = findzoneid(zone)
            except Exception:
                continue

            comp = find_comp_date(permit_date, dyear)
            rec["comp_date"] = comp.strftime('%m-%d-%Y')
            r = _c1_odds_on_date(zid, group_size, comp)
            if r is not None:
                rec["odds"], rec["obs"] = r

            season_start = dt.date(dyear, *SEASON_START_MD)
            season_end = dt.date(dyear, *SEASON_END_MD)
            for k in range(-neighbour_weeks, neighbour_weeks + 1):
                if k == 0:
                    continue
                nd = comp + dt.timedelta(days=7 * k)
                if nd < season_start or nd > season_end:
                    continue
                nr = _c1_odds_on_date(zid, group_size, nd)
                if nr is not None:
                    rec["neighbours"].append((k, nr[0], nr[1]))
        except Exception:
            pass
        finally:
            conn.close()

    return out


def smooth_year(rec: Dict[str, Any], weights: Dict[int, float] = SMOOTH_WEIGHTS):
    """Kernel-weighted odds for one year's record, or None if the comparable
    date itself has no data."""
    if rec.get("odds") is None:
        return None
    num = weights[0] * rec["odds"]
    den = weights[0]
    for k, p, _obs in rec.get("neighbours", []):
        w = weights.get(abs(k), 0.0)
        num += w * p
        den += w
    return num / den


def predict_first_choice(
    history: Dict[int, Dict[str, Any]],
    weights: Dict[int, float] = SMOOTH_WEIGHTS,
):
    """
    Calibrated first-choice prediction from lookup_first_choice_history()
    output: per-year neighbour-smoothed odds, averaged across years.
    Returns a float in [0, 1], or None when no year has data.
    """
    yearly = [v for v in (smooth_year(rec, weights) for rec in history.values()) if v is not None]
    if not yearly:
        return None
    return max(0.0, min(1.0, sum(yearly) / len(yearly)))
