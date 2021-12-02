# Package Design Structure

Guiding Principles:
1. Separation of Concerns
2. Loose Coupling

## Business Process (BP)
Single point of entry.  The API.

## Facade (FCD)
The Facade returns an instantiated Service for the consumer to invoke.  The Facade *does not* invoke the Service itself.

In Cendant, the Facade provides a "Complete Wrapper" around a service.  A service does "just one thing".  A facade facilitates the service, but augments the service with additional handling.   

Facade:
1. Parameter Validation
2. Initial Setup and Tear Down (such as stopwatch mechanisms or cache management) 
3. Additional Logging 
4. Additional Integration needs   

The Facade design pattern simplifies the interface to a complex system; because it is usually composed of all the classes which make up the subsystems of the complex system. 

References:
1. https://sourcemaking.com/design_patterns/facade

## Service (SVC)
A service is an event in a business process.  The service can call other services, and the service can call domain components.  Services do not contain business logic.

## Domain Component (DMO)
Domain Components contain business logic.  Domain components do not call each other.  Domain components do not call services.  