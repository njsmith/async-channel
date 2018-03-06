import time

import asyncio_channel
import curio_channel
import trio_channel


HOST = 'localhost'
PORT = 5555
CLIENT_COUNT = 30
MSG_COUNT = 1000


def main():
    print('Asyncio:')
    start = time.monotonic()
    asyncio_channel.main(HOST, PORT, CLIENT_COUNT, MSG_COUNT)
    print(f'{time.monotonic() - start:.3f}')
    print()

    print('Curio:')
    start = time.monotonic()
    curio_channel.main(HOST, PORT, CLIENT_COUNT, MSG_COUNT)
    print(f'{time.monotonic() - start:.3f}')
    print()

    print('Trio:')
    start = time.monotonic()
    trio_channel.main(HOST, PORT, CLIENT_COUNT, MSG_COUNT)
    print(f'{time.monotonic() - start:.3f}')


if __name__ == '__main__':
    main()
