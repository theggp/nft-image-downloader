
import os, time, random
import asyncio, aiohttp, aiofiles

import pandas as pd
from tqdm import tqdm
from params import n_workers

def logger(text):
    print(text)

def compose_url(project_contract, i):
    # Currently using rarible urls. Can be adapted to other urls.
    rarible_image_url = 'https://img.rarible.com/prod/image/upload/t_image_big/prod-itemImages/'
    image_png_url = rarible_image_url + project_contract + ':' + str(i)
    return image_png_url

async def get_image(session: aiohttp.ClientSession, url: str, **kwargs):
    resp = await session.request('GET', url=url, **kwargs)
    return resp

async def worker(session, queue, metadata, pbar):

    metadata_file, metadata_df, output_dir = metadata

    while True:
        # Get a "work item" out of the queue.
        id, image_url = await queue.get()

        pbar.update(1)
        if (metadata_df.loc[id].status != True):

            metadata_df.loc[id, 'image_url'] = image_url
            try:
                response = await get_image(session, image_url, )
                if response.status == 200:
                    f = await aiofiles.open(output_dir + str(id) + '.png', 'wb')
                    await f.write(await response.read())
                    await f.close()
                    metadata_df.loc[id, 'status'] = True
                else:
                    # Occasionaly some download fails, check manually the url and try again.
                    logger(f' > ERROR for url: {image_url}, response status = {response.status}')
                    metadata_df.loc[id, 'status'] = False

            except Exception as e:
                metadata_df.loc[id, 'status'] = False
                logger(e)

        if id % 100 == 0:
            # Saving the status every 100 downloads
            metadata_df.to_csv(metadata_file)

        await asyncio.sleep(random.uniform(0.02, 0.08))

        # Notify the queue that the "work item" has been processed.
        queue.task_done()


def metadata_setup(project_name, project_contract, n):

    output_dir = 'downloads/' + project_name + '/'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    metadata_dir = 'metadata'
    if not os.path.exists(metadata_dir):
        os.makedirs(metadata_dir)

    metadata_file = metadata_dir + '/' + project_name + '.csv'

    if not os.path.exists(metadata_file):
        cols = ['contract', 'image_url', 'status']
        metadata_df = pd.DataFrame(index=range(n), columns=cols)
        metadata_df['contract'] = project_contract
    else:
        metadata_df = pd.read_csv(metadata_file, index_col=0)

    return (metadata_file, metadata_df, output_dir)


async def launch_download(project_name, project_contract, n, batch_size):

    # Setup folders and metadata: metadata_file, metadata_df, output_dir
    metadata = metadata_setup(project_name, project_contract, n)

    # Create a queue that is used to store the workload.
    queue = asyncio.Queue()

    # checking previously failed downloads, will try again.
    metadata_df = metadata[1]
    previous_failures = (metadata_df.loc[:, ['status']] == False).sum()
    if (previous_failures[0]>0):
        logger(f'Previous failed downloads: {previous_failures[0]}. Retrying failed downloads.')

    # ids of the images to download in the next batch
    batch_ids = metadata_df.loc[(metadata_df.loc[:,['status']] != True)['status']].index[:batch_size]
    number_of_images = len(batch_ids)
    for id in batch_ids:
        image_url = compose_url(project_contract, id)
        queue.put_nowait((id, image_url))

    # Create workers tasks to process the queue concurrently.
    pbar = tqdm(total=number_of_images)
    async with aiohttp.ClientSession() as session:
        started_at = time.monotonic()

        tasks = []
        for i in range(n_workers):
            task = asyncio.create_task(worker(session, queue, metadata, pbar))
            tasks.append(task)
            await asyncio.sleep(random.uniform(0.01, 0.03))  # small initial delay between request

        # from this point forward as soon as an image is saved a new one will be requested
        await queue.join()

        # saving the status at end of the download
        metadata[1].to_csv(metadata[0])

        total_time = time.monotonic() - started_at

        for task in tasks:
            task.cancel()

    pbar.close()

    number_of_successful_downloads = (metadata_df.loc[:,['status']] == True).sum()[0]


    logger('==== Stats ====')
    logger(f' Max of {n_workers} concurrent downloads')
    logger(f' {number_of_images} images requests in {total_time:.2f} seconds')
    logger(f' average of {number_of_images/total_time:.2f} images requests per second')
    logger(f' collection downloaded: {number_of_successful_downloads}/{n} images')



