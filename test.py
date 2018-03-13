import time
from statistics import mean, stdev

import asyncio_channel
import curio_channel
import trio_channel


HOST = 'localhost'
PORT = 5555
CLIENT_COUNT = 30
MSG_COUNT = 1000


def run_channel_main(name, m):
    print(f'{name}:')
    latencies = []
    start = time.monotonic()
    m(HOST, PORT, CLIENT_COUNT, MSG_COUNT, latencies)
    print(f'{time.monotonic() - start:.3f}')
    print(f'latency: {1e3 * mean(latencies):.2f} +- {1e3 * stdev(latencies):.2f} ms')
    print(f'latency range: {1e3 * min(latencies):.2f} - {1e3 * max(latencies):.2f} ms')
    print()



def main():
    run_channel_main("Asyncio", asyncio_channel.main)
    run_channel_main("Curio", curio_channel.main)
    run_channel_main("Trio", trio_channel.main)


if __name__ == '__main__':
    main()
