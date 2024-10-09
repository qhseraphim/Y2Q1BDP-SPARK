package RDDAssignment

import java.util.UUID

import java.math.BigInteger
import java.security.MessageDigest

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import utils.{Commit, File, Stats}

object RDDAssignment {


  /**
    * Reductions are often used in data processing in order to gather more useful data out of raw data. In this case
    * we want to know how many commits a given RDD contains.
    *
    * @param commits RDD containing commit data.
    * @return Long indicating the number of commits in the given RDD.
    */
  def assignment_1(commits: RDD[Commit]): Long = ???

  /**
    * We want to know how often programming languages are used in committed files. We want you to return an RDD containing Tuples
    * of the used file extension, combined with the number of occurrences. If no filename or file extension is used we
    * assume the language to be 'unknown'.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing tuples indicating the programming language (extension) and number of occurrences.
    */
  def assignment_2(commits: RDD[Commit]): RDD[(String, Long)] = ???

  /**
    * Competitive users on GitHub might be interested in their ranking in the number of commits. We want you to return an
    * RDD containing Tuples of the rank (zero indexed) of a commit author, a commit author's name and the number of
    * commits made by the commit author. As in general with performance rankings, a higher performance means a better
    * ranking (0 = best). In case of a tie, the lexicographical ordering of the usernames should be used to break the
    * tie.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing the rank, the name and the total number of commits for every author, in the ordered fashion.
    */
  def assignment_3(commits: RDD[Commit]): RDD[(Long, String, Long)] = ???

  /**
    * Some users are interested in seeing an overall contribution of all their work. For this exercise we want an RDD that
    * contains the committer's name and the total number of their commit statistics. As stats are Optional, missing Stats cases should be
    * handled as "Stats(0, 0, 0)".
    *
    * Note that if a user is given that is not in the dataset, then the user's name should not occur in
    * the resulting RDD.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing committer names and an aggregation of the committers Stats.
    */
  def assignment_4(commits: RDD[Commit], users: List[String]): RDD[(String, Stats)] = ???


  /**
    * There are different types of people: those who own repositories, and those who make commits. Although Git blame command is
    * excellent in finding these types of people, we want to do it in Spark. As the output, we require an RDD containing the
    * names of commit authors and repository owners that have either exclusively committed to repositories, or
    * exclusively own repositories in the given commits RDD.
    *
    * Note that the repository owner is contained within GitHub URLs.
    *
    * @param commits RDD containing commit data.
    * @return RDD of Strings representing the usernames that have either only committed to repositories or only own
    *         repositories.
    */
  def assignment_5(commits: RDD[Commit]): RDD[String] = ???

  /**
    * Sometimes developers make mistakes and sometimes they make many many of them. One way of observing mistakes in commits is by
    * looking at so-called revert commits. We define a 'revert streak' as the number of times `Revert` occurs
    * in a commit message. Note that for a commit to be eligible for a 'revert streak', its message must start with `Revert`.
    * As an example: `Revert "Revert ...` would be a revert streak of 2, whilst `Oops, Revert Revert little mistake`
    * would not be a 'revert streak' at all.
    *
    * We require an RDD containing Tuples of the username of a commit author and a Tuple containing
    * the length of the longest 'revert streak' of a user and how often this streak has occurred.
    * Note that we are only interested in the longest commit streak of each author (and its frequency).
    *
    * @param commits RDD containing commit data.
    * @return RDD of Tuples containing a commit author's name and a Tuple which contains the length of the longest
    *         'revert streak' as well its frequency.
    */
  def assignment_6(commits: RDD[Commit]): RDD[(String, (Int, Int))] = ???


  /**
    * !!! NOTE THAT FROM THIS EXERCISE ON (INCLUSIVE), EXPENSIVE FUNCTIONS LIKE groupBy ARE NO LONGER ALLOWED TO BE USED !!
    *
    * We want to know the number of commits that have been made to each repository contained in the given RDD. Besides the
    * number of commits, we also want to know the unique committers that contributed to each of these repositories.
    *
    * In real life these wide dependency functions are performance killers, but luckily there are better performing alternatives!
    * The automatic graders will check the computation history of the returned RDDs.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples with the repository name, the number of commits made to the repository as
    *         well as the names of the unique committers to this repository.
    */
  def assignment_7(commits: RDD[Commit]): RDD[(String, Long, Iterable[String])] = ???

  /**
    * Return an RDD of Tuples containing the repository name and all the files that are contained in this repository.
    * Note that the file names must be unique, so if a file occurs multiple times (for example, due to removal, or new
    * addition), the newest File object must be returned. As the filenames are an `Option[String]`, discard the
    * files that do not have a filename.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing the files in each repository as described above.
    */
  def assignment_8(commits: RDD[Commit]): RDD[(String, Iterable[File])] = ???


  /**
    * For this assignment you are asked to find all the files of a single repository. This is in order to create an
    * overview of each file by creating a Tuple containing the file name, all corresponding commit SHA's,
    * as well as a Stat object representing all the changes made to the file.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples representing a file name, its corresponding commit SHA's and a Stats object
    *         representing the total aggregation of changes for a file.
    */
  def assignment_9(commits: RDD[Commit], repository: String): RDD[(String, Seq[String], Stats)] = ???

  /**
    * We want to generate an overview of the work done by a user per repository. For this we want an RDD containing
    * Tuples with the committer's name, the repository name and a `Stats` object containing the
    * total number of additions, deletions and total contribution to this repository.
    * Note that since Stats are optional, the required type is Option[Stat].
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples of the committer's name, the repository name and an `Option[Stat]` object representing additions,
    *         deletions and the total contribution to this repository by this committer.
    */
  def assignment_10(commits: RDD[Commit]): RDD[(String, String, Option[Stats])] = ???


  /**
    * Hashing function that computes the md5 hash of a String and returns a Long, representing the most significant bits of the hashed string.
    * It acts as a hashing function for repository name and username.
    *
    * @param s String to be hashed, consecutively mapped to a Long.
    * @return Long representing the MSB of the hashed input String.
    */
  def md5HashString(s: String): Long = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    UUID.nameUUIDFromBytes(hashedString.getBytes()).getMostSignificantBits
  }

  /**
    * Create a bi-directional graph from committer to repositories. Use the `md5HashString` function above to create unique
    * identifiers for the creation of the graph.
    *
    * Spark's GraphX library is actually used in the real world for algorithms like PageRank, Hubs and Authorities, clique finding, etc.
    * However, this is out of the scope of this course and thus, we will not go into further detail.
    *
    * We expect a node for each repository and each committer (based on committer name).
    * We expect an edge from each committer to the repositories that they have committed to.
    *
    * Look into the documentation of Graph and Edge before starting with this exercise.
    * Your vertices must contain information about the type of node: a 'developer' or a 'repository' node.
    * Edges must only exist between repositories and committers.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return Graph representation of the commits as described above.
    */
  def assignment_11(commits: RDD[Commit]): Graph[(String, String), String] = ???
}
