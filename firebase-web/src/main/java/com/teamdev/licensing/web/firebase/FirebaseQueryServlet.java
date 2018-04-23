/*
 * Copyright (c) 2000-2018 TeamDev Ltd. All rights reserved.
 * TeamDev PROPRIETARY and CONFIDENTIAL.
 * Use is subject to license terms.
 */

package com.teamdev.licensing.web.firebase;

import com.teamdev.licensing.web.QueryServlet;

/**
 * A {@link QueryServlet} which uses the {@link FirebaseQueryMediator}.
 *
 * @author Dmytro Dashenkov
 * @see QueryServlet
 */
@SuppressWarnings("serial") // Java serialization is not supported.
public abstract class FirebaseQueryServlet extends QueryServlet {

    /**
     * @see QueryServlet#QueryServlet
     */
    protected FirebaseQueryServlet(FirebaseQueryMediator mediator) {
        super(mediator);
    }
}
