package com.syncpulse.domain;

/**
 * Result of comparing two vector clocks under the causal ("happened-before")
 * partial order.
 *
 * <ul>
 *   <li>{@link #BEFORE}      — A happened-before B (A is causally in the past of B)</li>
 *   <li>{@link #AFTER}       — A happened-after B (A causally dominates B)</li>
 *   <li>{@link #EQUAL}       — identical causal history</li>
 *   <li>{@link #CONCURRENT}  — neither dominates the other (a genuine conflict)</li>
 * </ul>
 */
public enum Comparison {
    BEFORE,
    AFTER,
    EQUAL,
    CONCURRENT
}
