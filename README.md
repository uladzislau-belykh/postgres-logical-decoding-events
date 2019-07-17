# Postgres logical decoding events

"Postgres logical decoding events" java library that can read PostgrsSql WAL and provide changes in db as events. 
  Library contains two parts: 
- Event Replication - provaide managment for PostgresSql replication connections and streams. Guarantee only one produce of each event
- Event Holder - realization of load balancer for events. Was created for parallel handling replicated events.
