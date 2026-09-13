--How to Run a Flame Graph In Boom--

To run a flame graph, assuming we are working with example data 
from the README, we

1) install flamegraph with cargo, so run the command

    cargo install flamegraph

2) when starting your program, using the example code, instead of running

    cargo run --release --bin scheduler ztf

    Use the command

    taskset -c 0 cargo flamegraph --release --bin scheduler -- ztf

    We need to use taskset -c 0 to make sure we are running on only
    one core, as flamegraphs get messy on multiple threads.

3) After the program has ran, end the program. You should have a
flamegraph.svg that you can open in a browser.