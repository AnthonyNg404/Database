package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

//        System.out.println(effectiveLockType);
//        System.out.println(explicitLockType);
//        System.out.println(requestType);
//        System.out.println(parentContext);
//        System.out.println("break");

        // TODO(proj4_part2): implement

        if (LockType.substitutable(effectiveLockType, requestType) || LockType.substitutable(explicitLockType, requestType)) {
            return;
        }
        if (!LockType.substitutable(explicitLockType, requestType) && !LockType.substitutable(effectiveLockType, requestType) && explicitLockType == LockType.IX && requestType == LockType.S) {

            if (parentContext != null) {
                sufficientLockHeldAncestors(parentContext, LockType.parentLock(LockType.SIX));
            }
            lockContext.promote(transaction, LockType.SIX);

        } else if (!LockType.substitutable(explicitLockType, requestType) && !LockType.substitutable(effectiveLockType, requestType) && (explicitLockType == LockType.IS || explicitLockType == LockType.IX)) {
            if (parentContext != null) {
                sufficientLockHeldAncestors(parentContext, LockType.parentLock(requestType));
            }
            lockContext.escalate(transaction);

        } else if (!LockType.substitutable(explicitLockType, requestType) && !LockType.substitutable(effectiveLockType, requestType)) {
            if (parentContext != null) {
                sufficientLockHeldAncestors(parentContext, LockType.parentLock(requestType));
            }
            if (explicitLockType == LockType.NL) {
//                System.out.println("acquire lock");
                lockContext.acquire(transaction, requestType);
            } else {
                lockContext.promote(transaction, requestType);
            }
        }
        return;
    }

    private static void sufficientLockHeldAncestors(LockContext lockContext, LockType requestType) {
        LockContext parentContext = lockContext.parentContext();
        TransactionContext transaction = TransactionContext.getTransaction();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);
//        System.out.println(parentContext);
        if (parentContext != null) {
            sufficientLockHeldAncestors(parentContext, requestType);
        }
        if (!LockType.substitutable(explicitLockType, requestType) && !LockType.substitutable(effectiveLockType, requestType) && explicitLockType == LockType.NL) {
//            System.out.println(lockContext.getEffectiveLockType(transaction));
//            System.out.println(requestType);
            lockContext.acquire(transaction, requestType);
        } else if (!LockType.substitutable(explicitLockType, requestType) && !LockType.substitutable(effectiveLockType, requestType) && explicitLockType != requestType) {
//            System.out.println(effectiveLockType);
//            System.out.println(requestType);
            lockContext.promote(transaction, requestType);
        }
        return;
    }

    // TODO(proj4_part2) add any helper methods you want
}
