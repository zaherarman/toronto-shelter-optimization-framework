import numpy as np
import random

# ---------------------------------------
# Heuristics
# ---------------------------------------
def random_allocation(S, G, c, e, dist, d_g, rng):
    cap_left = {s: c[s] for s in S}
    sheltered = 0
    unsheltered = 0
    total_distance = 0.0
    for g in G:
        for _ in range(d_g[g]):
            candidates = [s for s in S if e[(s,g)] == 1 and cap_left[s] > 0]
            if not candidates:
                unsheltered += 1
                continue
            s_choice = rng.choice(candidates)
            cap_left[s_choice] -= 1
            sheltered += 1
            total_distance += dist[s_choice]
    return {"sheltered": sheltered, "unsheltered": unsheltered, "total_distance": total_distance}

def nearest_greedy(S, G, c, e, dist, d_g, rng):
    cap_left = {s: c[s] for s in S}
    sheltered = 0
    unsheltered = 0
    total_distance = 0.0
    for g in G:
        for _ in range(d_g[g]):
            candidates = [s for s in S if e[(s,g)] == 1 and cap_left[s] > 0]
            if not candidates:
                unsheltered += 1
                continue
            s_choice = min(candidates, key=lambda s: dist[s])
            cap_left[s_choice] -= 1
            sheltered += 1
            total_distance += dist[s_choice]
    return {"sheltered": sheltered, "unsheltered": unsheltered, "total_distance": total_distance}

# ---------------------------------------
# Daily demand sampler
# ---------------------------------------
def sample_daily_demand(month_index, G, monthly_by_gender, days_in_month, rng_np):
    month_totals = monthly_by_gender[month_index]
    lam = {g: month_totals[g] / days_in_month for g in G}
    return {g: rng_np.poisson(lam[g]) for g in G}

# ---------------------------------------
# Monte Carlo Engine
# ---------------------------------------
def monte_carlo(S, G, c, e, dist, monthly_by_gender, heuristics,
                month_index=0, n_days=1000, days_in_month=30, random_seed=123):
    rng_np = np.random.default_rng(random_seed)
    rng_py = random.Random(random_seed)
    results = {}
    for h in heuristics:
        results[h] = {"sheltered": [], "unsheltered": [], "total_distance": []}

    for _ in range(n_days):
        d_g_day = sample_daily_demand(month_index, G, monthly_by_gender, days_in_month, rng_np)
        for h_name, h_fn in heuristics.items():
            out = h_fn(S, G, c, e, dist, d_g_day, rng_py)
            results[h_name]["sheltered"].append(out["sheltered"])
            results[h_name]["unsheltered"].append(out["unsheltered"])
            results[h_name]["total_distance"].append(out["total_distance"])

    return results

# ---------------------------------------
# Data
# ---------------------------------------
G = [
    "men",
    "women"
]

S = [
    "973 Lansdowne Ave",
    "850 Bloor St W",
    "1651 Sheppard Ave W",
    "38 Bathurst St",
    "705 Progress Ave",
    "731 Runnymede Rd",
    "339 George St",
    "76 Church St",
    "674 Dundas St W",
    "616 Vaughan Rd",
    "349 George St",
    "386 Dundas St E",
    "512 Jarvis St",
    "67 Adelaide St E",
    "1059 College Street",
    "412 Queen St E",
    "35 Sydenham St",
    "702 Kennedy Rd",
    "14 Vaughan Rd",
    "26 Vaughan Rd",
    "962 Bloor St W",
    "126 Pape Ave",
    "60 Newcastle St",
    "70 Gerrard St E",
    "3410 Bayview Ave",
    "87 Pembroke St",
    "2808 Dundas St W",
    "107 Jarvis St",
    "135 Sherbourne St",
    "29A Leslie St",
    "2671 Islington Ave",
    "346 Spadina Ave.",
    "502 Spadina Ave",
    "80 Woodlawn Ave E",
    "348 Davenport Road"
]

c = {
    "512 Jarvis St": 28,
    "348 Davenport Road": 28,
    "502 Spadina Ave": 65,
    "70 Gerrard St E": 44,
    "349 George St": 30,
    "339 George St": 122,
    "346 Spadina Ave.": 71,
    "87 Pembroke St": 30,
    "80 Woodlawn Ave E": 31,
    "674 Dundas St W": 88,
    "386 Dundas St E": 37,
    "135 Sherbourne St": 264,
    "67 Adelaide St E": 52,
    "107 Jarvis St": 78,
    "76 Church St": 53,
    "14 Vaughan Rd": 54,
    "35 Sydenham St": 5,
    "26 Vaughan Rd": 17,
    "412 Queen St E": 35,
    "850 Bloor St W": 36,
    "38 Bathurst St": 68,
    "962 Bloor St W": 16,
    "1059 College Street": 36,
    "126 Pape Ave": 24,
    "973 Lansdowne Ave": 42,
    "616 Vaughan Rd": 28,
    "29A Leslie St": 52,
    "2808 Dundas St W": 86,
    "731 Runnymede Rd": 62,
    "60 Newcastle St": 41,
    "1651 Sheppard Ave W": 18,
    "702 Kennedy Rd": 61,
    "3410 Bayview Ave": 30,
    "2671 Islington Ave": 46,
    "705 Progress Ave": 101
}

e = {}
men_only = [
    "973 Lansdowne Ave",
    "850 Bloor St W",
    "1651 Sheppard Ave W",
    "38 Bathurst St",
    "705 Progress Ave",
    "731 Runnymede Rd",
    "339 George St",
    "76 Church St",
    "616 Vaughan Rd",
    "349 George St",
    "412 Queen St E",
    "35 Sydenham St",
    "14 Vaughan Rd",
    "26 Vaughan Rd",
    "107 Jarvis St",
    "135 Sherbourne St",
    "29A Leslie St",
    "2671 Islington Ave",
    "346 Spadina Ave.",
    "502 Spadina Ave"
]

women_only = [
    "674 Dundas St W",
    "386 Dundas St E",
    "512 Jarvis St",
    "67 Adelaide St E",
    "1059 College Street",
    "702 Kennedy Rd",
    "962 Bloor St W",
    "126 Pape Ave",
    "60 Newcastle St",
    "70 Gerrard St E",
    "3410 Bayview Ave",
    "87 Pembroke St",
    "2808 Dundas St W",
    "80 Woodlawn Ave E",
    "348 Davenport Road"
]

for s in men_only:
    e[(s,"men")] = 1
    e[(s,"women")] = 0

for s in women_only:
    e[(s,"men")] = 0
    e[(s,"women")] = 1

dist = {
    "512 Jarvis St": 0.9778932317042026,
    "348 Davenport Road": 1.1309174090050187,
    "502 Spadina Ave": 1.3086921105796272,
    "70 Gerrard St E": 1.3863911247008858,
    "349 George St": 1.6390483651607097,
    "339 George St": 1.670482508636295,
    "346 Spadina Ave.": 1.671288187435099,
    "87 Pembroke St": 1.8028318046617666,
    "80 Woodlawn Ave E": 1.8358389600864988,
    "674 Dundas St W": 2.0753002874685422,
    "386 Dundas St E": 2.1287228578113178,
    "135 Sherbourne St": 2.286022420867205,
    "67 Adelaide St E": 2.317295394322548,
    "107 Jarvis St": 2.3426292183615534,
    "76 Church St": 2.349838566145546,
    "14 Vaughan Rd": 2.6231083804920616,
    "35 Sydenham St": 2.6662125886876513,
    "26 Vaughan Rd": 2.674099501757083,
    "412 Queen St E": 2.6880854053791277,
    "850 Bloor St W": 2.7231762898874523,
    "38 Bathurst St": 3.060842441302751,
    "962 Bloor St W": 3.116770102477562,
    "1059 College Street": 3.6218320363280427,
    "126 Pape Ave": 4.3234509238481555,
    "973 Lansdowne Ave": 4.415402730126107,
    "616 Vaughan Rd": 4.713309356578003,
    "29A Leslie St": 5.092697798696732,
    "2808 Dundas St W": 5.797254555332866,
    "731 Runnymede Rd": 7.399278048600905,
    "60 Newcastle St": 10.218275618604487,
    "1651 Sheppard Ave W": 11.827495766539702,
    "702 Kennedy Rd": 12.059850994970251,
    "3410 Bayview Ave": 14.446273738400485,
    "2671 Islington Ave": 16.106719124859364,
    "705 Progress Ave": 16.905556379975213
}

monthly_by_gender = [
    {"men":4979, "women":2344},
    {"men":5083, "women":2408},
    {"men":4872, "women":2306},
    {"men":4775, "women":2297},
    {"men":4629, "women":2277},
    {"men":4507, "women":2250},
    {"men":4396, "women":2251},
    {"men":4397, "women":2251},
    {"men":4288, "women":2184},
    {"men":4259, "women":2200},
    {"men":4380, "women":2206},
    {"men":4925, "women":2334}
]

# ---------------------------------------
# Example run
# ---------------------------------------
if __name__ == "__main__":
    heuristics = {
        "random": random_allocation,
        "nearest": nearest_greedy
    }

    res = monte_carlo(S, G, c, e, dist, monthly_by_gender, heuristics,
                      month_index=0, n_days=300)

    import numpy as np
    for h, m in res.items():
        print("\nHeuristic:", h)
        print(" avg sheltered:", np.mean(m["sheltered"]))
        print(" avg unsheltered:", np.mean(m["unsheltered"]))
        print(" avg distance:", np.mean(m["total_distance"]))
