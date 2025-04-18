# hkademlia
This repository contains the core implementation files for the Hierarchical Kademlia (H-Kademlia) protocol designed to work with the PeerSim simulator.

## Files

- `HKademliaProtocol.java`: Main protocol class implementing H-Kademlia logic
- `HKademliaInitializer.java`: Assigns cluster IDs and initializes KBuckets
- `HKademliaStoreSimulator.java`: Simulates STORE/LOOKUP actions for evaluation

## Installation

To integrate with PeerSim:

1. Clone or download this repository.
2. Download the peersim tool. (https://sourceforge.net/projects/peersim/)
3. Make a custom config hkademlia txt file and place this in peersim-1.0.5/ directory. Also, place the contents of this main folder (`*.java` files) into the following path inside the PeerSim directory at /src/example/hkademlia.

