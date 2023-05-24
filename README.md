# Experimental Designs for Implementing Self-Organising Behaviours through Akka Typed actors

TODO DESCRIPTION

## Execution

```bash
./gradlew -PmainClass=it.unibo.aggrcompare.Actors1 run
```
- *Scenario*: we have a line topology of nodes with a neighbour distance of 1.
```
    // 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 10   (IDs)
    // --------------------------------------
    // 2 - 1 - 0 - 1 - 2 - 3 - 4 - 5 - 6 - 7    (gradient)
```
- *What should I see?* You should see that the nodes compute the right gradient value (i.e., the source device-3 returns 0, and e.g., device-8 returns 5).
  At some point (after about 10 seconds), device-4 is stopped: then, we should see the gradient values rising on the right side of the line.
