import psutil


def check_memory(throw: bool = False) -> bool:
    """Checks if the system is running low on memory."""

    # Get the virtual memory statistics
    mem = psutil.virtual_memory()

    # Define a threshold (e.g., 1% free memory)
    threshold = 1

    if mem.percent < threshold:
        if throw:
            raise MemoryError("Low memory detected!")
        return True  # Low memory
    else:
        return False  # Sufficient memory
