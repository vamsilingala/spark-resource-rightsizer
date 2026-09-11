# Open and commercial repositories

Maintain separate repositories, not long-lived edition branches. Release this core as a versioned dependency.
A commercial repository may add fleet inventory, persistence, dashboards, policy integrations, advanced cost
models, and controlled remediation without copying the core modules.

Generic fixes belong here first and flow to the commercial product through a tagged dependency upgrade. Client
connectors, deployments, and data stay in the commercial or client-specific repository.
