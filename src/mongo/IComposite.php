<?php
interface IComposite extends SplObserver
{
    /**
     * Returns the operation this composite can satisfy
     * @return string
     */
    public function getOperationType();

    /**
     * Returns the subjects that this composite will need to regenerate given changes made to the underlying dataset
     * @param $subjectsAndPredicatesOfChange
     * @param $deletedSubjects
     * @param $contextAlias
     * @return mixed
     */
    public function getModifiedSubjects(ChangeSet $cs,$deletedSubjects,$contextAlias);
}
