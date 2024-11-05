## PSA: This script is not ready for general use, as it still hardcoded to the specific use case of the project it was developed for.

# Standard library imports
import os
import time
import warnings
from queue import Queue
from threading import Thread
from typing import Union, Optional, Tuple, List, Callable

# Dependency imports
import torch
from torch.utils.data import DataLoader, IterableDataset#, TensorDataset
from torchvision.io import read_image
from torchvision.io.image import ImageReadMode

# Internal imports
from pyremotedata import main_logger, module_logger
from pyremotedata.implicit_mount import RemotePathIterator

class RemotePathDataset(IterableDataset):
    '''
    Creates a :py:class:`torch.utils.data.IterableDataset` from a :py:class:`pyremotedata.implicit_mount.RemotePathIterator`.

    By default the dataset will return the image as a tensor and the remote path as a string. 
    
    **Hierarchical mode**

    If `hierarchical` >= 1, the dataset is in "Hierarchical mode" and will return the image as a tensor and the label as a list of integers (class indices for each level in the hierarchy).
    
    The `class_handles` property can be used to get the class-idx mappings for the dataset.
    
    By default the dataset will use a parser which assumes that the hierarchical levels are encoded in the remote path as directories like so:
    
    `.../level_n/.../level_1/level_0/image.jpg`
    
    Where `n = (hierarchical - 1)` and `level_0` is the leaf level.

    Args:
        remote_path_iterator (RemotePathIterator): The :py:class:`pyremotedata.implicit_mount.RemotePathIterator` to create the dataset from.
        prefetch (int): The number of items to prefetch from the :py:class:`pyremotedata.implicit_mount.RemotePathIterator`.
        transform (callable, optional): A function/transform that takes in an image as a :py:class:`torch.Tensor` and returns a transformed version.
        target_transform (callable, optional): A function/transform that takes in the label (after potential parsing by `parse_hierarchical`) and transforms it.
        device (torch.device, optional): The device to move the tensors to.
        dtype (torch.dtype, optional): The data type to convert the tensors to.
        hierarchical (int, optional): The number of hierarchical levels to use for the labels. Default: 0, i.e. no hierarchy.
        hierarchy_parser (callable, optional): A function to parse the hierarchical levels from the remote path. Default: None, i.e. use the default parser.
        return_remote_path (bool, optional): Whether to return the remote path. Default: False.
        return_local_path (bool, optional): Whether to return the local path. Default: False.
        verbose (bool, optional): Whether to print verbose output. Default: False.

    Yields:
        (tuple): A tuple containing the following elements:
            - (torch.Tensor): The image as a tensor.
            - (Union[str, List[int]]): The label as the remote path or as a list of class indices.
            - (Optional[str]): The local path, if `return_local_path` is True.
            - (Optional[str]): The remote path, if `return_remote_path` is True.
    '''
    def __init__(
            self, 
            remote_path_iterator : "RemotePathIterator", 
            prefetch: int=64, 
            transform : Optional[Callable]=None, 
            target_transform : Optional[Callable]=None, 
            device: Union["torch.device", None]=None, 
            dtype: Union[torch.dtype, None]=None, 
            hierarchical: int=0, 
            hierarchy_parser: Optional[Callable]=None,
            shuffle: bool=False,
            return_remote_path: bool=False, 
            return_local_path: bool=False, 
            verbose: bool=False
        ):
        # Check if remote_path_iterator is of type RemotePathIterator
        if not isinstance(remote_path_iterator, RemotePathIterator):
            raise ValueError("Argument remote_path_iterator must be of type pyremotedata.implicit_mount.RemotePathIterator.")
        # Check if prefetch is an integer
        if not isinstance(prefetch, int):
            raise ValueError("Argument prefetch must be an integer.")
        # Check if prefetch is greater than 0
        if prefetch < 1:
            raise ValueError("Argument prefetch must be greater than 0.")

        ## General parameters
        assert isinstance(verbose, bool), ValueError("Argument verbose must be a boolean.")
        self.verbose : bool = verbose
        
        ## PyTorch specific parameters 
        # Get the classes and their indices
        self.hierarchical : int = hierarchical
        if self.hierarchical < 1:
            self.hierarchical = False
            def error_hierarchy(*args, **kwargs):
                raise ValueError("Hierarchical mode disabled (`hierarchical` < 1), but `pyremotedata.dataloader.RemotePathDataset.parse_hierarchy` function was called.")
            self.parse_hierarchy = error_hierarchy
        else:
            if hierarchy_parser is None:
                self.parse_hierarchy = lambda path: path.split('/')[-(1 + self.hierarchical):-1]
            elif callable(hierarchy_parser):
                self.parse_hierarchy = hierarchy_parser
            else:
                raise ValueError("Argument `hierarchy_parser` must be a callable or None.")
            self.classes = [[] for _ in range(self.hierarchical)]
            self.n_classes = [0 for _ in range(self.hierarchical)]
            self.class_to_idx = [{} for _ in range(self.hierarchical)]
            self.idx_to_class = [{} for _ in range(self.hierarchical)]
            for path in remote_path_iterator.remote_paths:
                for level, cls in enumerate(reversed(self.parse_hierarchy(path))):
                    if cls in self.classes[level]:
                        continue
                    if level >= self.hierarchical:
                        raise ValueError(f"Error parsing class from {path}. Got {cls} at level {level}, but the number of specified hierarchical levels is {self.hierarchical} with levels 0-{self.hierarchical-1}.")
                    self.classes[level].append(cls)
            for level in range(self.hierarchical):
                # self.classes[level] = sorted(list(set([path.split('/')[-2-level] for path in remote_path_iterator.remote_paths])))
                self.classes[level] = sorted(self.classes[level])
                self.n_classes[level] = len(self.classes[level])
                self.class_to_idx[level] = {self.classes[level][i]: i for i in range(len(self.classes[level]))}
                self.idx_to_class[level] = {i: self.classes[level][i] for i in range(len(self.classes[level]))}


        # Set the transforms
        self.transform = transform
        self.target_transform = target_transform

        # Set the device and dtype
        self.device = device
        self.dtype = dtype
        
        ## Backend specific parameters
        # Store the remote_path_iterator backend
        self.remote_path_iterator = remote_path_iterator
        self.return_remote_path = return_remote_path
        self.return_local_path = return_local_path
        self.shuffle = shuffle

        ## Multi-threading parameters
        # Set the number of workers (threads) for (multi)processing
        self.num_workers = 1

        # We don't want to start the buffer filling thread until the dataloader is called for iteration
        self.producer_thread = None
        # We need to keep track of whether the buffer filling thread has been initiated or not
        self.thread_initiated = False

        # Initialize the worker threads
        self.consumer_threads = []
        self.consumers = 0
        self.stop_consumer_threads = True
        
        # Set the buffer filling parameters (Watermark Buffering)
        self.buffer_minfill, self.buffer_maxfill = 0.4, 0.6

        # Initialize the buffers
        self.buffer = Queue(maxsize=prefetch)  # Tune maxsize as needed
        self.processed_buffer = Queue(maxsize=prefetch)  # Tune maxsize as needed

    @property
    def class_handles(self):
        return {
            'classes': self.classes,
            'n_classes': self.n_classes,
            'class_to_idx': self.class_to_idx,
            'idx_to_class': self.idx_to_class,
            'hierarchical': self.hierarchical
        }
    
    @class_handles.setter
    def class_handles(self, value : dict):
        if not isinstance(value, dict):
            raise ValueError("Argument value must be a dictionary.")
        if value["hierarchical"]:
            assert isinstance(value['classes'], list), ValueError("Argument value['classes'] must be a list, when hierarchical is True.")
        else:
            assert not isinstance(value['classes'], list), ValueError("Argument value['classes'] must not be a list, when hierarchical is False.")
        self.classes = value['classes']
        self.n_classes = value['n_classes']
        self.class_to_idx = value['class_to_idx']
        self.idx_to_class = value['idx_to_class']
        self.hierarchical = value['hierarchical']

    def _shuffle(self):
        if not self.shuffle:
            raise RuntimeError("Shuffle called, but shuffle is set to False.")
        if self.thread_initiated:
            raise RuntimeError("Shuffle called, but buffer filling thread is still active.")
        self.remote_path_iterator.shuffle()

    def _shutdown_and_reset(self):
        # Handle shutdown logic (Should probably be moved to a dedicated reset function, that is called on StopIteration instead or perhaps in __iter__)
        self.stop_consumer_threads = True # Signal the consumer threads to stop
        for i, consumer in enumerate(self.consumer_threads):
            if consumer is None:
                continue
            if not consumer.is_alive():
                continue
            main_logger.debug(f"Waiting for worker {i} to finish.")
            consumer.join(timeout = 1 / self.num_workers) # Wait for the consumer thread to finish
        if self.producer_thread is not None:
            self.producer_thread.join(timeout=1) # Wait for the producer thread to finish
            self.producer_thread = None # Reset the producer thread
        self.consumer_threads = [] # Reset the consumer threads
        self.consumers = 0 # Reset the number of consumers
        self.thread_initiated = False # Reset the thread initiated flag
        self.buffer.queue.clear() # Clear the buffer
        self.processed_buffer.queue.clear() # Clear the processed buffer
        self.remote_path_iterator.__del__(force=True)  # Close the remote_path_iterator and clean the temporary directory
        assert self.buffer.qsize() == 0, RuntimeError("Buffer not empty after iterator end.")
        assert self.processed_buffer.qsize() == 0, RuntimeError("Processed buffer not empty after iterator end.")

    def _init_buffer(self):
        # Check if the buffer filling thread has been initiated
        if not self.thread_initiated:
            # Start the buffer filling thread
            self.producer_thread = Thread(target=self._fill_buffer)
            self.producer_thread.daemon = True
            self.producer_thread.start()
            # Set the flag to indicate that the thread has been initiated
            self.thread_initiated = True
        else:
            # Raise an error if the buffer filling thread has already been initiated
            raise RuntimeError("Buffer filling thread already initiated.")

    def _fill_buffer(self):
        # Calculate the min and max fill values for the buffer
        min_fill = int(self.buffer.maxsize * self.buffer_minfill) 
        max_fill = int(self.buffer.maxsize * self.buffer_maxfill)

        main_logger.debug(f"Buffer min fill: {min_fill}, max fill: {max_fill}")
        main_logger.debug("Producer thread started.")

        wait_for_min_fill = False
        for item in self.remote_path_iterator:
            # Get the current size of the buffer
            current_buffer_size = self.buffer.qsize()

            # Decide whether to fill the buffer based on its current size
            if not wait_for_min_fill:
                wait_for_min_fill = (current_buffer_size >= max_fill)

            # Sleep logic which ensures that the buffer doesn't switch between filling and not filling too often (Watermark Buffering)
            while wait_for_min_fill:
                time.sleep(0.1)
                current_buffer_size = self.buffer.qsize() # Update the current buffer size
                wait_for_min_fill = current_buffer_size >= min_fill # Wait until the buffer drops below min_fill

            # Fill the buffer
            self.buffer.put(item)
        
        main_logger.debug("Producer signalling end of iterator.")
        
        # Signal the end of the iterator to the consumers by putting None in the buffer until all consumer threads have finished
        while self.consumers > 0:
            time.sleep(0.01)
            self.buffer.put(None)
        main_logger.debug("Producer emptying buffer.")
        
        # Wait for the consumer threads to finish then clear the buffer
        while self.buffer.qsize() > 0:
            self.buffer.get()
        self.processed_buffer.put(None) # Signal the end of the processed buffer to the main thread
        main_logger.debug("Producer thread finished.")
    
    def _process_buffer(self):
        main_logger.debug("Consumer thread started.")
        
        self.consumers += 1
        while True:
            qsize = self.buffer.qsize()
            while qsize < (self.num_workers * 2):
                time.sleep(0.05 / self.num_workers)
                qsize = self.buffer.qsize()
                if self.producer_thread is None or not self.producer_thread.is_alive():
                    break
            # Get the next item from the buffer
            item = self.buffer.get()
            main_logger.debug("Consumer thread got item")
            
            # Check if the buffer is empty, signaling the end of the iterator
            if item is None or self.stop_consumer_threads:
                break  # Close the thread

            # Preprocess the item (e.g. read image, apply transforms, etc.) and put it in the processed buffer
            processed_item = self.parse_item(*item) if item is not None else None
            if processed_item is not None:
                self.processed_buffer.put(processed_item)
            main_logger.debug("Consumer thread processed item")

        self.consumers -= 1
        main_logger.debug("Consumer thread finished.")

    def __iter__(self):
        # Check if the buffer filling thread has been initiated
        # If it has, reset the dataloader state and close all threads
        # (Only one iteration is allowed per dataloader instance)
        if self.thread_initiated: 
            warnings.warn("Iterator called, but buffer filling thread is still active. Resetting the dataloader state.")
        self._shutdown_and_reset()

        # If shuffle is set to True, shuffle the remote_path_iterator
        if self.shuffle:
            self._shuffle()
        
        # Initialize the buffer filling thread
        self._init_buffer()
        
        # Check number of workers
        if self.num_workers == 0:
            self.num_workers = 1
        if self.num_workers < 1:
            raise ValueError("Number of workers must be greater than 0.")

        self.stop_consumer_threads = False

        # Start consumer threads for processing
        for _ in range(self.num_workers):
            consumer_thread = Thread(target=self._process_buffer)
            consumer_thread.daemon = True
            consumer_thread.start()
            self.consumer_threads.append(consumer_thread)

        return self

    def __next__(self):
        # Fetch from processed_buffer instead
        processed_item = self.processed_buffer.get()

        # Check if the processed buffer is empty, signaling the end of the iterator
        if processed_item is None:
            self._shutdown_and_reset()
            raise StopIteration
        
        # Restart crashed consumer threads
        for thread in self.consumer_threads:
            if not thread.is_alive():
                self.consumer_threads.remove(thread)
                new_consumer_thread = Thread(target=self._process_buffer)
                new_consumer_thread.daemon = True
                self.consumer_threads.append(new_consumer_thread)

        # Otherwise, return the processed item
        return processed_item

    def __len__(self):
        return len(self.remote_path_iterator)
    
    def parse_item(self, local_path : str, remote_path : str) -> Union[Tuple[torch.Tensor, Union[str, List[int]]], Tuple[torch.Tensor, Union[str, List[int]], str], Tuple[torch.Tensor, Union[str, List[int]], str, str]]:
        ## Image processing
        # Check if image format is supported (jpeg/jpg/png)
        image_type = os.path.splitext(local_path)[-1]
        if image_type not in ['.JPG', '.JPEG', '.PNG', '.jpg', '.jpeg', '.png']:
            # raise ValueError(f"Image format of {remote_path} ({image_type}) is not supported.")
            # Instead of raising an error, we can skip the image instead
            return None
        try:
            image = read_image(local_path, mode=ImageReadMode.RGB)
        except Exception as e:
            main_logger.error(f"Error reading image {remote_path} ({e}).")
            return None
        # Remove the alpha channel if present
        if image.shape[0] == 4:
            image = image[:3]
        if image.shape[0] != 3:
            main_logger.error(f"Error reading image {remote_path}.")
            return None
        # Apply transforms (preprocessing)
        if self.transform:
            image = self.transform(image)
        if self.dtype is not None:
            image = image.to(dtype=self.dtype)
        if self.device is not None:
            image = image.to(device=self.device)
        
        if self.hierarchical:
            ## Label processing
            # Get the label by parsing the remote path
            hierarchy = self.parse_hierarchy(remote_path)
            # Transform the species name to the label index
            label = [self.class_to_idx[level][cls] for level, cls in enumerate(hierarchy)]
            if len(label) == 0:
                raise ValueError(f"Error parsing label from {remote_path}.")
        else:
            label = remote_path

        # Apply label transforms
        if self.target_transform:
            label = self.target_transform(label)
        # TODO: Does the label need to be converted to a tensor and moved to the device? (Probably not) If so, does it need to be a long tensor?
        # if self.device is not None:
        #     label = label.to(device=self.device)

        ## Return the image and label (and optionally the local and remote paths)
        if self.return_local_path and self.return_remote_path:
            return image, label, local_path, remote_path
        elif self.return_local_path:
            return image, label, local_path
        elif self.return_remote_path:
            return image, label, remote_path
        else:
            return image, label

class RemotePathDataLoader(DataLoader):
    """
    A custom :py:class:`torch.utils.data.DataLoader` for :py:class:`pyremotedata.dataloader.RemotePathDataset`.

    This DataLoader is designed to work with :py:class:`pyremotedata.dataloader.RemotePathDataset` and does not support all the arguments of the standard :py:class:`torch.utils.data.DataLoader`.

    Unsupported arguments:
    - sampler
    - batch_sampler

    Args:
        dataset (RemotePathDataset): The :py:class:`pyremotedata.dataloader.RemotePathDataset` dataset to load from.
        num_workers (int, optional): The number of worker threads to use for loading. Default: 0. Must be greater than 0.
        shuffle (bool, optional): Whether to shuffle the dataset between epochs. Default: False.
    """
    def __init__(self, dataset: "RemotePathDataset", num_workers : int=0, shuffle : bool=False, *args, **kwargs):
        # Snipe arguments from the user which would break the custom dataloader (e.g. sampler, shuffle, etc.)
        unsupported_kwargs = ['sampler', 'batch_sampler']
        for unzkw in unsupported_kwargs:
            value = kwargs.pop(unzkw, None)
            if value is not None:
                warnings.warn(f"Argument {unzkw} is not supported in `pyremotedata.dataloader.RemotePathDataLoader`. {unzkw}={value} will be ignored.")

        # Override the num_workers argument handling (default is 0)
        dataset.num_workers = num_workers
        # Override the shuffle argument handling (default is False)
        dataset.shuffle = shuffle
        
        if not isinstance(dataset, RemotePathDataset):
            raise ValueError("Argument dataset must be of type `pyremotedata.dataloader.RemotePathDataset`.")

        # Initialize the dataloader
        super(RemotePathDataLoader, self).__init__(
            shuffle=False,
            sampler=None,
            batch_sampler=None,
            num_workers=0,
            dataset=dataset,
            *args, 
            **kwargs
        )
    
    # TODO: This cannot be set before calling super().__init__(), so perhaps it can be overridden after initialization instead
    # def __setattr__(self, name, value):
    #     if name in ['batch_sampler', 'sampler']:
    #         raise (f"Changing {name} is not allowed in this custom DataLoader.")
    #     super(RemotePathDataLoader, self).__setattr__(name, value)