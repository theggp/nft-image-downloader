
import platform, asyncio
from params import project_name, project_contract, n, batch_size
from async_image_downloader import launch_download

if __name__ == '__main__':

    if platform.system() == 'Windows':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(launch_download(project_name, project_contract, n, batch_size))