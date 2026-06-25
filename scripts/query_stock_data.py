"""Optional local helper to inspect generated files outside Airflow."""
from pathlib import Path
import sys
import pandas as pd


def main(execution_date: str):
    data_dir = Path('/tmp/market_data') / execution_date
    frames = []
    for symbol in ['AAPL', 'TSLA']:
        df = pd.read_csv(data_dir / f'{symbol}.csv')
        df['symbol'] = symbol
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True)
    close_col = 'Close' if 'Close' in combined.columns else 'close'
    print(combined.groupby('symbol')[close_col].agg(['mean', 'max', 'min', 'count']))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        raise SystemExit('Usage: python scripts/query_stock_data.py YYYY-MM-DD')
    main(sys.argv[1])
