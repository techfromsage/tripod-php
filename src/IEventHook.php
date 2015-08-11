<?php
/**
 * Created by PhpStorm.
 * User: chrisc
 * Date: 05/08/2015
 * Time: 13:36
 */

namespace Tripod;

/**
 * Interface IEventHook
 * @package Tripod
 */
interface IEventHook
{
    /**
     * Constant for save changes event.
     * IEventHook::pre() will be passed the following parameters in a multidimensional array:
     *   - oldGraph - the graph state before the change
     *   - newGraph - the graph state after the change is successfully applied
     *
     * IEventHook::pre() will be passed the following parameters in a multidimensional array:
     *   - subjectsAndPredicatesOfChange - an array, keyed by subject, with the subjects and predicates changed during the save
     */
    const EVENT_SAVE_CHANGES = "EVENT_SAVE_CHANGES";

    /**
     * This method gets called just before the event happens. The arguments passed depend on the event in question, see
     * the documentation for that event type for details
     * @param $args array of arguments
     */
    public function pre(array $args);

    /**
     * This method gets called after the event has successfully completed. The arguments passed depend on the event in
     * question, see the documentation for that event type for details
     * If the event throws an exception or fatal error, this method will not be called.
     * @param $args array of arguments
     */
    public function post(array $args);

    /**
     * This method gets called if the event failed for any reason. The arguments passed should be the same as IEventHook::pre
     * @param array $args
     * @return mixed
     */
    public function failure(array $args);
}