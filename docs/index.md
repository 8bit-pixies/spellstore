---
title: Spellstore
---

# What is Spellstore?

Spellstore is a Feature Store built upon SQLAlchemy. It provides a SDK for building Machine Learning Systems suitable for batch workflows, which allow for consistent interfaces between training and scoring models. 

# Problems Spellstore solves

*  Models need consistent access to data
*  Models need point-in-time correct data
*  Justifying new infrastructure for many organisations is difficult

# What Spellstore is not

*  Spellstore does not aim to replace infrastructure heavy approaches to Feature Store - better alternatives exist such as Feast

# Design Philosophy

*  Spellstore focuses on a small set of CLI-based programs to deliver ML solutions, and follows the UNIX mindset
*  To operate effectively, Spellstore should never need to write data anywhere - though optional utilities may be provided to ease the transition
