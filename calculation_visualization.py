import sqlite3
from scipy.stats import pearsonr, linregress
import numpy as np
import matplotlib.pyplot as plt

DB_NAME = "final_project.db"

def create_connection():
    return sqlite3.connect(DB_NAME)

def fetch_joined_data(conn):
    """
    Get joined rows: one per book–movie pair.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT
            B.book_title,
            B.book_rating,
            B.ratings_count,
            M.movie_title,
            M.movie_rating,
            M.movie_count
        FROM Book_Movie AS BM
        JOIN Books  AS B ON BM.book_id  = B.book_id
        JOIN Movies AS M ON BM.movie_id = M.movie_id
    """)
    rows = cur.fetchall()

    data = []
    for row in rows:
        data.append({
            "book_title": row[0],
            "book_rating": row[1],    
            "book_count": row[2],
            "movie_title": row[3],
            "movie_rating": row[4],     
            "movie_count": row[5]
        })

    return data

def filter_data(data, min_book_count=0, min_movie_count=0):
    filtered = []
    for row in data:
        brc = row["book_count"]
        mvc = row["movie_count"]

        if brc is None or mvc is None:
            continue

        if brc >= min_book_count and mvc >= min_movie_count:
            filtered.append(row)

    return filtered


def convert_movie_rating(movie_rating_10):
    """Convert IMDb rating 1–10 to 1–5."""
    if movie_rating_10 is None:
        return None
    return movie_rating_10 / 2.0


def compute_preference_counts(data):
    books_better = 0
    movies_better = 0
    ties = 0
    total = 0

    for row in data:
        book_rating = row.get("book_rating")        # FIXED
        movie_rating_10 = row.get("movie_rating")   # FIXED
        movie_rating_5 = convert_movie_rating(movie_rating_10)

        if book_rating is None or movie_rating_5 is None:
            continue

        total += 1
        if movie_rating_5 > book_rating:
            movies_better += 1
        elif book_rating > movie_rating_5:
            books_better += 1
        else:
            ties += 1

    return books_better, movies_better, ties, total

def prefence_percentage(preferece_counts):
    books_better, movies_better, ties, total = preferece_counts

    if total == 0:
        return (0, 0, 0)

    books_better_pct = books_better / total
    movies_better_pct = movies_better / total
    ties_pct = ties / total
    return books_better_pct, movies_better_pct, ties_pct

def preference_pie (preference_pct):
    labels = ["Movies preferred", "Books preferred", "Ties"]
    sizes = [
        preference_pct[1],   # movies
        preference_pct[0],   # books
        preference_pct[2]    # ties
    ]

    fig, ax = plt.subplots()
    ax.pie(
        sizes,
        labels=labels,
        autopct="%1.1f%%",
        startangle=90,
        colors=["blue"] * len(sizes)   # all-blue slices
    )
    ax.axis("equal")
    ax.set_title("Preference: Adapted Movies vs Original Books")
    plt.tight_layout()
    return fig

def preference_bar(preference_pct):
    """
    Create a bar chart comparing the prefered percentage.
    """
    labels = ["Movies preferred", "Books preferred", "Ties"]
    sizes = [
        preference_pct[1],   # movies
        preference_pct[0],   # books
        preference_pct[2]    # ties
    ]

    fig, ax = plt.subplots()

    ax.bar(labels, sizes, autopct="%1.1f%%", colors=["blue"] * len(sizes))
    ax.set_ylabel("Percentage (%)")
    ax.set_title("Preference: Adapted Movies vs Original Books")

    plt.tight_layout()
    return fig

def prepare_correlation_data(data):
    x_values = []
    y_values = []

    for row in data:
        book_rating = row.get("book_rating")
        movie_rating_10 = row.get("movie_rating")
        movie_rating_5 = convert_movie_rating(movie_rating_10)

        if book_rating is None or movie_rating_5 is None:
            continue

        x_values.append(float(book_rating))
        y_values.append(float(movie_rating_5))

    return x_values, y_values

def pearson_correlation(x, y):
    if len(x) != len(y) or len(x) < 3:
        return None

    r, p = pearsonr(np.array(x), np.array(y))
    return r, p

def linear_regression(x, y):
    if len(x) != len(y) or len(x) < 3:
        return None

    result = linregress(np.array(x), np.array(y))
    return {
        "slope": result.slope,
        "intercept": result.intercept,
        "r_value": result.rvalue,
        "p_value": result.pvalue,
        "stderr": result.stderr
    }

def correlation_scatter(x, y, r=None, p=None, reg=None):
    x_arr = np.array(x)
    y_arr = np.array(y)

    fig, ax = plt.subplots()
    ax.scatter(x_arr, y_arr, alpha=0.8)

    if reg is not None:
        line_x = np.linspace(min(x_arr), max(x_arr), 100)
        line_y = reg["slope"] * line_x + reg["intercept"]
        ax.plot(line_x, line_y, "--", linewidth=2, label="Regression line")

    ax.set_xlabel("Book average rating (1-5)")
    ax.set_ylabel("Movie IMDb rating (scaled to 1-5)")
    ax.set_title("Book Ratings vs Movie Ratings")
    ax.grid(True)

    if r is not None and p is not None:
        ax.text(
            0.05, 0.95,
            f"r = {r:.3f}\np = {p:.3g}",
            transform=ax.transAxes,
            fontsize=10,
            verticalalignment="top",
            bbox=dict(facecolor="white", alpha=0.7, edgecolor="gray")
        )

    if reg is not None:
        ax.legend()

    plt.tight_layout()
    return fig

def correlation_hexbin(x, y):
    """
    Hexbin plot for book vs movie ratings.
    Shows density of overlapping points.
    """
    x_arr = np.array(x, dtype=float)
    y_arr = np.array(y, dtype=float)

    fig, ax = plt.subplots()

    hb = ax.hexbin(
        x_arr,
        y_arr,
        gridsize=20,      
        cmap='Blues',     
        mincnt=1,         
        linewidths=0.2
    )

    cb = fig.colorbar(hb, ax=ax)
    cb.set_label('Count')

    ax.set_xlabel("Book average rating (1–5)")
    ax.set_ylabel("Movie IMDb rating (scaled to 1–5)")
    ax.set_title("Book Ratings vs Movie Ratings (Hexbin Density)")

    plt.tight_layout()
    return fig

def write_summary_file(
    filename: str,
    preference_counts=None,
    preference_pct=None,
    correlation_result=None,
    regression_result=None):
    lines = []
    lines.append("=== Analysis Summary ===\n")

    if preference_counts is not None:
        books_better, movies_better, ties, total = preference_counts
        lines.append("Book vs Movie Preference (Question 1):")
        lines.append(f"  Total comparisons: {total}")
        lines.append(f"  Books preferred:  {books_better}")
        lines.append(f"  Movies preferred: {movies_better}")
        lines.append(f"  Ties:             {ties}\n")

    if preference_pct is not None:
        lines.append("Preference Percentages:")
        lines.append(f"  Books better %:   {preference_pct[0]:.3f}")
        lines.append(f"  Movies better %:  {preference_pct[1]:.3f}")
        lines.append(f"  Ties %:           {preference_pct[2]:.3f}\n")

    if correlation_result is not None:
        r, p = correlation_result
        lines.append("Correlation (Question 2):")
        lines.append(f"  Pearson r: {r:.4f}")
        lines.append(f"  p-value:   {p:.6f}\n")

    if regression_result is not None:
        lines.append("Regression (Question 2):")
        lines.append(f"  Slope:      {regression_result['slope']:.4f}")
        lines.append(f"  Intercept:  {regression_result['intercept']:.4f}")
        lines.append(f"  r-value:    {regression_result['r_value']:.4f}")
        lines.append(f"  p-value:    {regression_result['p_value']:.6f}")
        lines.append(f"  std error:  {regression_result['stderr']:.6f}\n")

    with open(filename, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

def main():
    conn = create_connection()
    data = fetch_joined_data(conn)
    filtered_data = filter_data(data)

    # Question 1
    preference_counts = compute_preference_counts(filtered_data)
    preference_pct = prefence_percentage(preference_counts)

    # Question 2
    x_values, y_values = prepare_correlation_data(filtered_data)
    correlation_result = pearson_correlation(x_values, y_values)
    regression_result = linear_regression(x_values, y_values)

    write_summary_file(
        "analysis_summary.txt",
        preference_counts=preference_counts,
        preference_pct=preference_pct,
        correlation_result=correlation_result,
        regression_result=regression_result
    )
    print("Saved summary: analysis_summary.txt")

    pie_fig = preference_pie(preference_pct)
    pie_fig.savefig("preference_pie_chart.png")
    print("Saved preference pie: preference_pie_chart.png")

    stacked_pref_fig = preference_bar(filtered_data)
    stacked_pref_fig.savefig("stacked_preference_chart.png")
    print("Saved chart: stacked_preference_chart.png")

    fig = correlation_scatter(
            x_values,
            y_values,
            r=correlation_result[0] if correlation_result else None,
            p=correlation_result[1] if correlation_result else None,
            reg=regression_result
        )
    fig.savefig("scatter_plot.png")
    print("Saved scatter plot: scatter_plot.png")

    hex_fig = correlation_hexbin(x_values, y_values)
    hex_fig.savefig("hexbin_plot.png")
    print("Saved hexbin plot: hexbin_plot.png")

    conn.close()

if __name__ == "__main__":
    main()
