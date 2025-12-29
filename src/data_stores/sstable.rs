struct sstable<S> {
    state: PhantomData<S>,

    page_size: usize,
}