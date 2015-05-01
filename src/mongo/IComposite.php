<?php
interface IComposite
{
    /**
     * Returns the operation this composite can satisfy
     * @return string
     */
    public function getOperationType();

    /**
     * Returns the subjects that this composite will need to regenerate given changes made to the underlying dataset
     * @param array $subjectsAndPredicatesOfChange
     * @param string $contextAlias
     * @return mixed
     */
    public function getImpactedSubjects(Array $subjectsAndPredicatesOfChange,$contextAlias);

    public function update(ImpactedSubject $subject);
}
