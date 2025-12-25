from __future__ import annotations

from typing import Any, Dict, List, Tuple

import pandas as pd

from src.optimization.model import build_model_multi_hotspot


def run_referral_simulation(
    monthly_demands: List[Dict[Any, Dict[Any, float]]],
    hotspots: List[Any],
    shelters: List[Any],
    genders: List[Any],
    capacities: Dict[Any, float],
    eligibility: Dict[Tuple[Any, Any], int],
    distances: Dict[Tuple[Any, Any], float],
    print_utilization: bool = True,
    return_dfs: bool = True,
):
    """
    Combined runner that preserves BOTH output styles:

    - Monthly preview prints (Top 10 assignments / Top 10 unsheltered)
    - Monthly results list includes raw_assignments/raw_unsheltered
    - Year-wide df_x_final/df_z_final across all months (if return_dfs=True)
    """
    
    results: List[Dict[str, Any]] = []

    # Master lists to hold every decision variable record across all months
    all_x_records: List[Dict[str, Any]] = []
    all_z_records: List[Dict[str, Any]] = []

    for month_idx, demand_data in enumerate(monthly_demands, start=1):

        # Flatten demand into (h, g) -> value
        demand_flat: Dict[Tuple[Any, Any], float] = {}
        for h in hotspots:
            for g in genders:
                demand_flat[(h, g)] = float(demand_data[h][g])

        # Run optimization
        model, variables, info = build_model_multi_hotspot(
            hotspots=hotspots,
            shelters=shelters,
            genders=genders,
            demand=demand_flat,
            capacities=capacities,
            dist=distances,
            eligibility=eligibility,
        )

        x_vars = variables["x"]
        z_vars = variables["z"]

        # Month-scoped lists (snippet 1 structure)
        assignments: List[Dict[str, Any]] = []
        unsheltered_list: List[Dict[str, Any]] = []

        # Collect assignments (x)
        for h in hotspots:
            for s in shelters:
                for g in genders:
                    val = x_vars[(h, s, g)].X
                    if val > 0.5:
                        rec = {
                            "Month": month_idx,
                            "Origin_Hotspot": h,
                            "Dest_Shelter": s,
                            "Gender": g,
                            "People_Moved": int(val),
                            "Distance_km": distances.get((h, s), 0),
                        }
                        assignments.append(rec)
                        all_x_records.append(rec)

        # Collect unsheltered (z)
        for h in hotspots:
            for g in genders:
                val = z_vars[(h, g)].X
                if val > 0.5:
                    rec = {
                        "Month": month_idx,
                        "Hotspot": h,
                        "Gender": g,
                        "People_Left_Behind": int(val),
                    }
                    unsheltered_list.append(rec)
                    all_z_records.append(rec)

        # Monthly preview prints
        if print_utilization:
            print(f"\n{'='*20} MONTH {month_idx} DECISIONS {'='*20}")

            if assignments:
                df_x_preview = pd.DataFrame(assignments)
                print(f"\n--- Top 10 Assignments (Total Moves: {len(df_x_preview)}) ---")
                print(df_x_preview.head(10).to_string(index=False))
            else:
                print("No assignments made.")

            if unsheltered_list:
                df_z_preview = pd.DataFrame(unsheltered_list)
                print(f"\n--- Top 10 Unsheltered Locations (Total Locations: {len(df_z_preview)}) ---")
                print(
                    df_z_preview.sort_values(by="People_Left_Behind", ascending=False)
                    .head(10)
                    .to_string(index=False)
                )
            else:
                print("Everyone was housed!")

        # Summary stats
        total_assigned_count = sum(x_vars[(h, s, g)].X for h in hotspots for s in shelters for g in genders)
        avg_dist_month = (info["obj_distance"] / total_assigned_count) if total_assigned_count > 0 else 0.0

        gender_stats: Dict[Any, Dict[str, float]] = {}
        for g in genders:
            g_unsheltered = sum(z_vars[(h, g)].X for h in hotspots)
            g_assigned = sum(x_vars[(h, s, g)].X for h in hotspots for s in shelters)

            g_total_dist = 0.0
            for h in hotspots:
                for s in shelters:
                    flow = x_vars[(h, s, g)].X
                    if flow > 0.001:
                        g_total_dist += flow * distances.get((h, s), 999.0)

            g_avg_dist = (g_total_dist / g_assigned) if g_assigned > 0 else 0.0
            gender_stats[g] = {"assigned": g_assigned, "unsheltered": g_unsheltered, "avg_dist": g_avg_dist}

        shelter_usage_stats: Dict[Any, Dict[str, float]] = {}
        for s in shelters:
            used_amount = sum(x_vars[(h, s, g)].X for h in hotspots for g in genders)
            cap = float(capacities[s])
            pct_full = (used_amount / cap) * 100.0 if cap > 0 else 0.0
            shelter_usage_stats[s] = {"used": used_amount, "capacity": cap, "utilization_pct": pct_full}

        month_result = {
            "month": month_idx,
            "total_unsheltered": info["Z_star"],
            "total_distance": info["obj_distance"],
            "total_assigned": total_assigned_count,
            "overall_avg_distance": avg_dist_month,
            "shelter_usage": shelter_usage_stats,
            "gender_breakdown": gender_stats,
            "raw_assignments": assignments,
            "raw_unsheltered": unsheltered_list,
        }
        results.append(month_result)

    if return_dfs:
        df_x_final = pd.DataFrame(all_x_records)
        df_z_final = pd.DataFrame(all_z_records)
        return results, df_x_final, df_z_final

    return results


if __name__ == "__main__":
    from src.config import PROCESSED_DATA_DIR

    print("Loading Data...")
    df_demand = pd.read_csv(PROCESSED_DATA_DIR / "demand_per_hotspot.csv")
    df_dist = pd.read_csv(PROCESSED_DATA_DIR / "distance_amtrix.csv")
    df_capacity = pd.read_csv(PROCESSED_DATA_DIR / "capacities_per_shelter.csv")
    df_genders = pd.read_csv(PROCESSED_DATA_DIR / "eligibility_df.csv")

    print("Processing Data...")
    genders = ["men", "women"]

    # Hotspots: all distance-matrix columns
    hotspots = [c for c in df_dist.columns if c != "LOCATION_NAME" and not c.startswith("Unnamed")]

    # Shelters + capacity + eligibility
    shelters: List[int] = []
    capacities: Dict[int, float] = {}
    eligibility: Dict[Tuple[int, str], int] = {}
    location_name_to_ids: Dict[str, List[int]] = {}

    for idx, row_cap in df_capacity.iterrows():
        s_id = int(idx)
        name = row_cap["SHELTER_NAME"]
        cap = float(row_cap["CAPACITY"])

        # Align gender row by name (keeps your previous defensive behavior)
        row_elig = df_genders.iloc[idx]
        if row_elig["SHELTER_NAME"] != name:
            matching = df_genders[df_genders["SHELTER_NAME"] == name]
            if matching.empty:
                print(f"Warning: Could not find gender info for {name}")
                continue
            row_elig = matching.iloc[0]

        shelters.append(s_id)
        capacities[s_id] = cap
        eligibility[(s_id, "men")] = int(row_elig["men_eligible_eligible"])
        eligibility[(s_id, "women")] = int(row_elig["women_eligible_eligible"])

        location_name_to_ids.setdefault(name, []).append(s_id)

    # Distances
    distances: Dict[Tuple[Any, int], float] = {}
    for _, row in df_dist.iterrows():
        loc_name = row["LOCATION_NAME"]
        if loc_name in location_name_to_ids:
            for h in hotspots:
                if h in df_dist.columns:
                    dist_val = float(row[h])
                    for s_id in location_name_to_ids[loc_name]:
                        distances[(h, s_id)] = dist_val

    # Monthly demands: list of dict[hotspot][gender]
    monthly_demands: List[Dict[Any, Dict[str, float]]] = []
    for m in sorted(df_demand["month"].unique()):
        df_m = df_demand[df_demand["month"] == m]
        month_dict: Dict[Any, Dict[str, float]] = {}
        for _, row in df_m.iterrows():
            h = row["CFSAUID"]
            if h in hotspots:
                month_dict[h] = {
                    "men": float(row["male_from_hotspot"]),
                    "women": float(row["female_from_hotspot"]),
                }
        monthly_demands.append(month_dict)

    print("\nStarting Simulation...")
    summary_results, df_x, df_z = run_referral_simulation(
        monthly_demands=monthly_demands,
        hotspots=hotspots,
        shelters=shelters,
        genders=genders,
        capacities=capacities,
        eligibility=eligibility,
        distances=distances,
        print_utilization=True,
        return_dfs=True,
    )

    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 1000)

    print("\n" + "#" * 80)
    print("FULL DECISION VARIABLE LIST: X (ASSIGNMENTS)")
    print("#" * 80)
    print(df_x.to_string(index=False) if not df_x.empty else "No assignments made.")

    print("\n" + "#" * 80)
    print("FULL DECISION VARIABLE LIST: Z (UNSHELTERED)")
    print("#" * 80)
    print(df_z.to_string(index=False) if not df_z.empty else "Zero unsheltered people!")

    pd.reset_option("display.max_rows")
    pd.reset_option("display.max_columns")
    pd.reset_option("display.width")
